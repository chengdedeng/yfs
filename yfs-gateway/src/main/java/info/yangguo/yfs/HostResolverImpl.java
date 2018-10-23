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

import info.yangguo.yfs.config.ClusterConfig;
import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import lombok.Getter;
import lombok.Setter;
import org.littleshoot.proxy.HostResolver;

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
    private volatile static HostResolverImpl singleton;
    private ClusterConfig clusterConfig;
    @Getter
    @Setter
    private WeightedRoundRobinScheduling uploadServers;
    @Getter
    @Setter
    private Map<String, WeightedRoundRobinScheduling> downloadServers;

    private HostResolverImpl(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.uploadServers = new WeightedRoundRobinScheduling();
        this.downloadServers = new ConcurrentHashMap<>();
    }

    public static HostResolverImpl getSingleton(ClusterConfig clusterConfig) {
        if (singleton == null) {
            synchronized (HostResolverImpl.class) {
                if (singleton == null) {
                    singleton = new HostResolverImpl(clusterConfig);
                }
            }
        }
        return singleton;
    }

    @Override
    public InetSocketAddress resolve(String host, int port)
            throws UnknownHostException {
        WeightedRoundRobinScheduling.Server server = null;
        //上传域名比下载域名长一段
        //例如：上传域名为yfs.info，下载域名为group1.yfs.info。
        if (host.equals(clusterConfig.clusterProperties.dn)) {
            server = uploadServers.getServer();
        } else if (host.endsWith(clusterConfig.clusterProperties.dn)) {
            WeightedRoundRobinScheduling weightedRoundRobinScheduling = downloadServers.get(host.split("\\.")[0]);
            server = weightedRoundRobinScheduling.getServer();
        }
        if (server != null) {
            return new InetSocketAddress(server.getStoreInfo().getIp(), server.getStoreInfo().getStoreHttpPort());
        } else {
            throw new UnknownHostException("host:" + host + ",port:" + port);
        }
    }
}
