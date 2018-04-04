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
import info.yangguo.yfs.util.PropertiesUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;

import java.util.List;
import java.util.Map;

/**
 * @author:杨果
 * @date:2017/4/11 下午1:52
 * <p>
 * Description:
 */
public class Constant {
    public static Map<String, String> gatewayConfs = PropertiesUtil.getProperty("gateway.properties");
    public static int AcceptorThreads = Integer.parseInt(gatewayConfs.get("gateway.acceptorThreads"));
    public static int ClientToProxyWorkerThreads = Integer.parseInt(gatewayConfs.get("gateway.clientToProxyWorkerThreads"));
    public static int ProxyToServerWorkerThreads = Integer.parseInt(gatewayConfs.get("gateway.proxyToServerWorkerThreads"));
    public static int ServerPort = Integer.parseInt(gatewayConfs.get("gateway.serverPort"));

    public static String getRealIp(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext) {
        List<String> headerValues = getHeaderValues(httpRequest, "X-Real-IP");
        return headerValues.get(0);
    }

    /**
     * RFC7230/RFC7231/RFC7232/RFC7233/RFC7234
     * Each header field consists of a case-insensitive field name followed
     * by a colon (":"), optional leading whitespace, the field value, and
     * optional trailing whitespace.
     *
     * @param httpMessage
     * @param headerName
     * @return headerValue
     */
    public static List<String> getHeaderValues(HttpMessage httpMessage, String headerName) {
        List<String> list = Lists.newArrayList();
        for (Map.Entry<String, String> header : httpMessage.headers().entries()) {
            if (header.getKey().toLowerCase().equals(headerName.toLowerCase())) {
                list.add(header.getValue());
            }
        }
        return list;
    }
}
