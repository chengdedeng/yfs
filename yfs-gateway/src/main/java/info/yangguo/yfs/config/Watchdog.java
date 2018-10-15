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
import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import io.atomix.cluster.ClusterMembershipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Watchdog implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    private ClusterConfig clusterConfig;

    public Watchdog(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000 * 5);
                logger.debug("watchdog**************************start");
                ClusterMembershipService membershipService = clusterConfig.atomix.getMembershipService();
                membershipService.getMembers().stream().forEach(member -> {
                    if (!member.isReachable()) {
                        removeStoreBySocketPort(member.address().host(), member.address().port());
                        logger.error("nodeId:{},ip:{},port:{}", member.id(), member.address().host(), member.address().port());
                    }
                });
                updateDownload();
            } catch (Exception e) {
                logger.error("watchdog error", e);
            }
            logger.debug("watchdog****************************end");
        }
    }

    /**
     * 更新下载节点
     */
    public void updateDownload() {
        clusterConfig.atomicMap.entrySet().stream().forEach(entry -> {
            StoreInfo storeInfo = entry.getValue().value();
            String group = storeInfo.getGroup();
            if (!HostResolverImpl.downloadServers.containsKey(group)) {
                HostResolverImpl.downloadServers.put(group, new WeightedRoundRobinScheduling());
            }
            WeightedRoundRobinScheduling weightedRoundRobinScheduling = HostResolverImpl.downloadServers.get(group);
            AtomicBoolean isFind = new AtomicBoolean(false);
            for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinScheduling.healthilyServers) {
                if (server.getStoreInfo().getNodeId().equals(storeInfo.getNodeId())) {
                    isFind.set(true);
                    break;
                }
            }
            if (!isFind.get()) {
                WeightedRoundRobinScheduling.Server server = new WeightedRoundRobinScheduling.Server(storeInfo, 1);
                weightedRoundRobinScheduling.healthilyServers.add(server);
            }
        });
        updateUpload();
    }

    /**
     * 通过IP和StoreHttpPort端口删除store节点，ip+port能够确认唯一的store节点，并且一个store只能属于一个group
     *
     * @param ip
     * @param storeHttpPort
     */
    public void removeStoreByHttpPort(String ip, int storeHttpPort) {
        for (Map.Entry<String, WeightedRoundRobinScheduling> weightedRoundRobinSchedulingEntry : HostResolverImpl.downloadServers.entrySet()) {
            for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinSchedulingEntry.getValue().healthilyServers) {
                if (server.getStoreInfo().getIp().equals(ip) && server.getStoreInfo().getStoreHttpPort() == storeHttpPort) {
                    clusterConfig.atomicMap.remove(CommonConstant.storeInfoConsistentMapKey(server.getStoreInfo().getGroup(), server.getStoreInfo().getIp(), server.getStoreInfo().getStoreHttpPort()));
                    weightedRoundRobinSchedulingEntry.getValue().healthilyServers.remove(server);
                    updateUpload();
                    logger.warn("store server has been removed:\n:", JsonUtil.toJson(server, true));
                    return;
                }
            }
        }
    }

    /**
     * 通过IP和GatwaySocketPort端口删除store节点，ip+port能够确认唯一的store节点，并且一个store只能属于一个group
     *
     * @param ip
     * @param gatwaySocketPort
     */
    public void removeStoreBySocketPort(String ip, int gatwaySocketPort) {
        for (Map.Entry<String, WeightedRoundRobinScheduling> weightedRoundRobinSchedulingEntry : HostResolverImpl.downloadServers.entrySet()) {
            for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinSchedulingEntry.getValue().healthilyServers) {
                if (server.getStoreInfo().getIp().equals(ip) && server.getStoreInfo().getGatewaySocketPort() == gatwaySocketPort) {
                    clusterConfig.atomicMap.remove(CommonConstant.storeInfoConsistentMapKey(server.getStoreInfo().getGroup(), server.getStoreInfo().getIp(), server.getStoreInfo().getStoreHttpPort()));
                    weightedRoundRobinSchedulingEntry.getValue().healthilyServers.remove(server);
                    updateUpload();
                    logger.warn("store server has been removed:\n:", JsonUtil.toJson(server, true));
                    return;
                }
            }
        }
    }

    /**
     * 检查并更新上传服务器列表
     */
    private void updateUpload() {
        List<WeightedRoundRobinScheduling.Server> uploadServers = Lists.newArrayList();
        HostResolverImpl.downloadServers.entrySet().stream().forEach(entry -> {
            //上传的时候，每个group服务器的数量最少得是两台
            if (entry.getValue().healthilyServers.size() > 1) {
                AtomicLong minStoreSpace = new AtomicLong();
                minStoreSpace.set(Long.MAX_VALUE);
                //获取store存储空间的最小值
                entry.getValue().healthilyServers.stream().forEach(server -> {
                    StoreInfo storeInfo = clusterConfig.atomicMap.get(CommonConstant.storeInfoConsistentMapKey(server.getStoreInfo().getGroup(), server.getStoreInfo().getIp(), server.getStoreInfo().getStoreHttpPort())).value();
                    if (storeInfo != null && storeInfo.getFileFreeSpaceKb() < minStoreSpace.get()) {
                        minStoreSpace.set(storeInfo.getFileFreeSpaceKb());
                    }
                });
                entry.getValue().healthilyServers.stream().forEach(server -> {
                    Long weight = minStoreSpace.get() % clusterConfig.clusterProperties.getProtected_space();
                    try {
                        WeightedRoundRobinScheduling.Server tmp = (WeightedRoundRobinScheduling.Server) server.clone();
                        tmp.setWeight(weight.intValue());
                        uploadServers.add(server);
                    } catch (CloneNotSupportedException e) {
                        new RuntimeException(e);
                    }
                });
            }
        });
        HostResolverImpl.uploadServers.healthilyServers = uploadServers;
        logger.debug("update servers of upload");
    }
}
