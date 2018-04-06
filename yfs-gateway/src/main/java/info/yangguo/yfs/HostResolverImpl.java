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

import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import org.littleshoot.proxy.HostResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author:杨果
 * @date:2017/3/31 下午7:28
 * <p>
 * Description:
 */
public class HostResolverImpl implements HostResolver {
    private static Logger logger = LoggerFactory.getLogger(HostResolverImpl.class);
    private volatile static HostResolverImpl singleton;
    public static final WeightedRoundRobinScheduling uploadServers = new WeightedRoundRobinScheduling();
    public static final Map<String, WeightedRoundRobinScheduling> downloadServers = new ConcurrentHashMap<>();

    private HostResolverImpl() {
    }

    public static HostResolverImpl getSingleton() {
        if (singleton == null) {
            synchronized (HostResolverImpl.class) {
                if (singleton == null) {
                    singleton = new HostResolverImpl();
                }
            }
        }
        return singleton;
    }

    @Override
    public InetSocketAddress resolve(String host, int port)
            throws UnknownHostException {
        String[] items = host.split("\\.");
        WeightedRoundRobinScheduling.Server server = null;
        if (items.length == 2) {
            server = uploadServers.getServer();
        } else if (items.length == 3) {
            WeightedRoundRobinScheduling weightedRoundRobinScheduling = downloadServers.get(items[0]);
            server = weightedRoundRobinScheduling.getServer();
        }
        if (server != null) {
            return new InetSocketAddress(server.getStoreInfo().getIp(), server.getStoreInfo().getStoreHttpPort());
        } else {
            throw new UnknownHostException("host:" + host + ",port:" + port);
        }
    }
}
