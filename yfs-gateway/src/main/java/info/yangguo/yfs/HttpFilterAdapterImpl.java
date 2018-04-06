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

import info.yangguo.yfs.config.Watchdog;
import info.yangguo.yfs.request.CCHttpRequestFilter;
import info.yangguo.yfs.request.HttpRequestFilter;
import info.yangguo.yfs.request.HttpRequestFilterChain;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.impl.ClientToProxyConnection;
import org.littleshoot.proxy.impl.ProxyToServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
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
    private static final HttpRequestFilterChain httpRequestFilterChain = new HttpRequestFilterChain();

    static {
        if (Constant.gatewayConfs.get("gateway.cc").equals("on")) {
            httpRequestFilterChain.addFilter(new CCHttpRequestFilter());
        }
    }


    public HttpFilterAdapterImpl(HttpRequest originalRequest, ChannelHandlerContext ctx) {
        super(originalRequest, ctx);
    }


    @Override
    public HttpResponse clientToProxyRequest(HttpObject httpObject) {
        HttpResponse httpResponse = null;
        try {
            ImmutablePair<Boolean, HttpRequestFilter> immutablePair = httpRequestFilterChain.doFilter(originalRequest, httpObject, ctx);
            if (immutablePair.left) {
                httpResponse = createResponse(HttpResponseStatus.FORBIDDEN, originalRequest);
            }
        } catch (Exception e) {
            httpResponse = createResponse(HttpResponseStatus.BAD_GATEWAY, originalRequest);
            logger.error("client's request failed", e.getCause());
        }
        return httpResponse;
    }

    @Override
    public void proxyToServerResolutionSucceeded(String serverHostAndPort,
                                                 InetSocketAddress resolvedRemoteAddress) {
        if (resolvedRemoteAddress == null) {
            //在使用 Channel 写数据之前，建议使用 isWritable() 方法来判断一下当前 ChannelOutboundBuffer 里的写缓存水位，防止 OOM 发生。不过实践下来，正常的通信过程不太会 OOM，但当网络环境不好，同时传输报文很大时，确实会出现限流的情况。
            if (ctx.channel().isWritable()) {
                ctx.writeAndFlush(createResponse(HttpResponseStatus.BAD_GATEWAY, originalRequest));
            }
        }
    }

    @Override
    public void proxyToServerConnectionFailed() {
        ClientToProxyConnection clientToProxyConnection = (ClientToProxyConnection) ctx.handler();
        try {
            Field field = ClientToProxyConnection.class.getDeclaredField("currentServerConnection");
            field.setAccessible(true);
            ProxyToServerConnection proxyToServerConnection = (ProxyToServerConnection) field.get(clientToProxyConnection);
            String remoteHostName = proxyToServerConnection.getRemoteAddress().getAddress().getHostAddress();
            int remoteHostPort = proxyToServerConnection.getRemoteAddress().getPort();
            Watchdog.removeStoreByHttpPort(remoteHostName, remoteHostPort);
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

    private static HttpResponse createResponse(HttpResponseStatus httpResponseStatus, HttpRequest originalRequest) {
        HttpHeaders httpHeaders = new DefaultHttpHeaders();
        httpHeaders.add("Transfer-Encoding", "chunked");
        HttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus);

        //support CORS
        List<String> originHeader = Constant.getHeaderValues(originalRequest, "Origin");
        if (originHeader.size() > 0) {
            httpHeaders.set("Access-Control-Allow-Credentials", "true");
            httpHeaders.set("Access-Control-Allow-Origin", originHeader.get(0));
        }
        httpResponse.headers().add(httpHeaders);
        return httpResponse;
    }
}
