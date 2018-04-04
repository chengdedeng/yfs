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
package info.yangguo.yfs.config;

import com.google.common.collect.Lists;
import info.yangguo.yfs.HostResolverImpl;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import io.atomix.core.map.ConsistentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Watchdog implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    private static final long lifeTime = 1000 * 15;
    private ConsistentMap<String, StoreInfo> consistentMap;

    public Watchdog(ConsistentMap<String, StoreInfo> consistentMap) {
        this.consistentMap = consistentMap;
    }

    @Override
    public void run() {
        while (true) {
            logger.debug("watchdog**************************server");
            try {
                Thread.sleep(1000 * 5);
                consistentMap.asJavaMap().entrySet().stream().forEach(entry -> {
                    StoreInfo storeInfo = entry.getValue();
                    String group = storeInfo.getGroup();
                    if (!HostResolverImpl.downloadServers.containsKey(group)) {
                        HostResolverImpl.downloadServers.put(group, new WeightedRoundRobinScheduling());
                    }
                    WeightedRoundRobinScheduling weightedRoundRobinScheduling = HostResolverImpl.downloadServers.get(group);
                    if (new Date().getTime() - storeInfo.getUpdateTime() < lifeTime) {
                        AtomicBoolean isSet = new AtomicBoolean(false);
                        for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinScheduling.healthilyServers) {
                            if (server.getGroup().equals(storeInfo.getGroup()) && server.getId().equals(storeInfo.getNodeId())) {
                                server.setWeight(storeInfo.getFileFreeSpaceKb().intValue());
                                isSet.set(true);
                                break;
                            }
                        }
                        if (!isSet.get()) {
                            WeightedRoundRobinScheduling.Server server = new WeightedRoundRobinScheduling.Server(storeInfo.getGroup(), storeInfo.getNodeId(), storeInfo.getHost(), storeInfo.getHttp_port(), storeInfo.getFileFreeSpaceKb().intValue());
                            weightedRoundRobinScheduling.healthilyServers.add(server);
                        }
                    } else {
                        for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinScheduling.healthilyServers) {
                            if (server.getGroup().equals(storeInfo.getGroup()) && server.getId().equals(storeInfo.getNodeId())) {
                                weightedRoundRobinScheduling.healthilyServers.remove(server);
                                break;
                            }
                        }
                    }
                });
                updateUpload();
            } catch (Exception e) {
                logger.error("watchdog error", e);
            }
            logger.debug("server**************************watchdog");
        }
    }

    public static void removeStore(String host, int port) {
        for (Map.Entry<String, WeightedRoundRobinScheduling> weightedRoundRobinSchedulingEntry : HostResolverImpl.downloadServers.entrySet()) {
            for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinSchedulingEntry.getValue().healthilyServers) {
                if (server.getIp().equals(host) && server.getPort() == port) {
                    weightedRoundRobinSchedulingEntry.getValue().healthilyServers.remove(server);
                    updateUpload();
                    logger.warn("store server[group:{},ip:{},port:{}] has been removed", server.getGroup(), server.getIp(), server.getPort());
                    return;
                }
            }
        }
    }

    private static void updateUpload() {
        List<WeightedRoundRobinScheduling.Server> tmp = Lists.newArrayList();
        HostResolverImpl.downloadServers.entrySet().stream().forEach(entry -> {
            //上传的时候，每个group服务器的数量最少得是两台
            if (entry.getValue().healthilyServers.size() > 1) {
                tmp.addAll(entry.getValue().healthilyServers);
            }
        });
        HostResolverImpl.uploadServers.healthilyServers = tmp;
        logger.debug("update servers of upload");
    }
}
