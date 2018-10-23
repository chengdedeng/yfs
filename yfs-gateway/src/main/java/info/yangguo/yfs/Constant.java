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

import info.yangguo.yfs.util.PropertiesUtil;

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
    public static int IdleConnectionTimeout = Integer.parseInt(gatewayConfs.get("gateway.idleConnectionTimeout"));
}
