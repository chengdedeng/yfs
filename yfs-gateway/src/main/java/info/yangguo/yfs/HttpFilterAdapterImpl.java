/*
 * Copyright 2018-present yangguo@outlook.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.yangguo.yfs;

import com.google.common.collect.Lists;
import info.yangguo.yfs.config.ClusterConfig;
import info.yangguo.yfs.config.Watchdog;
import info.yangguo.yfs.request.RequestFilter;
import info.yangguo.yfs.request.RewriteFilter;
import info.yangguo.yfs.util.ResponseUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ClientToProxyConnection;
import org.littleshoot.proxy.impl.ProxyConnection;
import org.littleshoot.proxy.impl.ProxyToServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author:杨果
 * @date:2017/4/17 下午2:12
 * <p>
 * Description:
 */
public class HttpFilterAdapterImpl extends HttpFiltersAdapter {
    private static Logger logger = LoggerFactory.getLogger(HttpFilterAdapterImpl.class);

    private Watchdog watchdog;
    private ClusterConfig clusterConfig;


    public HttpFilterAdapterImpl(HttpRequest originalRequest, ChannelHandlerContext ctx, Watchdog watchdog, ClusterConfig clusterConfig) {
        super(originalRequest, ctx);
        this.watchdog = watchdog;
        this.clusterConfig = clusterConfig;
    }


    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        //放到里面主要是为了线程安全，由于一条链路不断的情况下，多个请求过来都在一个ClientToProxy线程中，但是对于Filter来说确实多线程处理的，
        //不放在里面就会报对List操作的操作异常。
        List<RequestFilter> requestFilters = Lists.newArrayList();
        //注意顺序
        requestFilters.add(new RewriteFilter(clusterConfig));

        HttpResponse response = null;
        for (RequestFilter filter : requestFilters) {
            try {
                response = filter.doFilter(originalRequest, httpObject);
            } catch (Exception e) {
                logger.warn("request client to proxy failed", e);
                response = ResponseUtil.createResponse(HttpResponseStatus.BAD_GATEWAY, originalRequest, null);
            }
            if (response != null) {
                break;
            }
        }

        return response;
    }

    @Override
    public void proxyToServerResolutionSucceeded(String serverHostAndPort,
                                                 InetSocketAddress resolvedRemoteAddress) {
        if (resolvedRemoteAddress == null) {
            //在使用 Channel 写数据之前，建议使用 isWritable() 方法来判断一下当前 ChannelOutboundBuffer 里的写缓存水位，防止 OOM 发生。不过实践下来，正常的通信过程不太会 OOM，但当网络环境不好，同时传输报文很大时，确实会出现限流的情况。
            if (ctx.channel().isWritable()) {
                ctx.writeAndFlush(ResponseUtil.createResponse(HttpResponseStatus.BAD_GATEWAY, originalRequest, null));
            }
        }
    }

    @Override
    public void proxyToServerConnectionFailed() {
        try {
            ClientToProxyConnection clientToProxyConnection = (ClientToProxyConnection) ctx.handler();
            ProxyToServerConnection proxyToServerConnection = clientToProxyConnection.getProxyToServerConnection();
            String remoteHostName = proxyToServerConnection.getRemoteAddress().getAddress().getHostAddress();
            int remoteHostPort = proxyToServerConnection.getRemoteAddress().getPort();
            watchdog.removeStoreByHttpPort(remoteHostName, remoteHostPort);
        } catch (Exception e) {
            logger.error("connection of proxy->server is failed", e);
        }
    }

    @Override
    public void proxyToServerConnectionSucceeded(final ChannelHandlerContext serverCtx) {
        ChannelPipeline pipeline = serverCtx.pipeline();
        //当没有修改getMaximumResponseBufferSizeInBytes中buffer默认的大小时,下面两个handler是不存在的
        if (pipeline.get("inflater") != null) {
            pipeline.remove("inflater");
        }
        if (pipeline.get("aggregator") != null) {
            pipeline.remove("aggregator");
        }
        super.proxyToServerConnectionSucceeded(serverCtx);
    }

    @Override
    public void proxyToServerRequestSending() {
        ClientToProxyConnection clientToProxyConnection = (ClientToProxyConnection) ctx.handler();
        ProxyConnection proxyConnection = clientToProxyConnection.getProxyToServerConnection();
        logger.debug("client channel:{}-{}", clientToProxyConnection.getChannel().localAddress().toString(), clientToProxyConnection.getChannel().remoteAddress().toString());
        logger.debug("server channel:{}-{}", proxyConnection.getChannel().localAddress().toString(), proxyConnection.getChannel().remoteAddress().toString());
        proxyConnection.getChannel().closeFuture().addListener(new GenericFutureListener() {
            @Override
            public void operationComplete(Future future) {
                if (clientToProxyConnection.getChannel().isActive()) {
                    logger.debug("channel:{}-{} will be closed", clientToProxyConnection.getChannel().localAddress().toString(), clientToProxyConnection.getChannel().remoteAddress().toString());
                    clientToProxyConnection.getChannel().close();
                } else {
                    logger.debug("channel:{}-{} has been closed", clientToProxyConnection.getChannel().localAddress().toString(), clientToProxyConnection.getChannel().remoteAddress().toString());
                }
            }
        });
    }
}
