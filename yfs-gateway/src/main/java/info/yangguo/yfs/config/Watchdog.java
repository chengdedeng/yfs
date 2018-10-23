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
import com.google.common.collect.Maps;
import info.yangguo.yfs.HostResolverImpl;
import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
                Thread.sleep(clusterConfig.clusterProperties.detectionCycle * 1000);
                ClusterMembershipService membershipService = clusterConfig.atomix.getMembershipService();
                List<Member> healthMembers = Lists.newArrayList();
                membershipService.getMembers().stream().forEach(member -> {
                    if (member.isActive() && member.isReachable() && CommonConstant.storeZone.equals(member.zone())) {
                        healthMembers.add(member);
                    }
                });
                clusterConfig.atomicMap.entrySet().forEach(entry -> {
                    StoreInfo storeInfo = entry.getValue().value();
                    boolean isHealth = false;
                    for (Member member : healthMembers) {
                        if (storeInfo.getGroup().equals(member.config().getRack())
                                && storeInfo.getNodeId().equals(member.id().id())) {
                            isHealth = true;
                            break;
                        }
                    }
                    if (!isHealth) {
                        logger.warn("store:{}下线", JsonUtil.toJson(storeInfo, false));
                        removeStoreBySocketPort(storeInfo.getIp(), storeInfo.getStoreSocketPort());
                    }
                });
                updateDownload();
            } catch (Exception e) {
                logger.error("watchdog error", e);
            }
            logger.debug("store****************************watchdog");
        }
    }

    /**
     * 更新下载节点
     */
    public void updateDownload() {
        Map<String, WeightedRoundRobinScheduling> newDownloadServers = Maps.newHashMap();
        clusterConfig.atomicMap.entrySet().stream().forEach(entry -> {
            StoreInfo storeInfo = entry.getValue().value();
            String group = storeInfo.getGroup();
            WeightedRoundRobinScheduling.Server server = new WeightedRoundRobinScheduling.Server(storeInfo, 1);
            if (newDownloadServers.containsKey(group)) {
                newDownloadServers.get(group).healthilyServers.add(server);
            } else {
                WeightedRoundRobinScheduling weightedRoundRobinScheduling = new WeightedRoundRobinScheduling();
                weightedRoundRobinScheduling.healthilyServers.add(server);
                newDownloadServers.put(group, weightedRoundRobinScheduling);
            }
        });
        HostResolverImpl.getSingleton(clusterConfig).getDownloadServers().putAll(newDownloadServers);
        Iterator<Map.Entry<String, WeightedRoundRobinScheduling>> iterator = HostResolverImpl.getSingleton(clusterConfig).getDownloadServers().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, WeightedRoundRobinScheduling> entry = iterator.next();
            if (!newDownloadServers.containsKey(entry.getKey())) {
                iterator.remove();
            }
        }
        updateUpload();
    }

    /**
     * 通过IP和StoreHttpPort端口删除store节点，ip+port能够确认唯一的store节点，并且一个store只能属于一个group。
     *
     * @param ip
     * @param storeHttpPort
     */
    public void removeStoreByHttpPort(String ip, int storeHttpPort) {
        for (Map.Entry<String, WeightedRoundRobinScheduling> weightedRoundRobinSchedulingEntry : HostResolverImpl.getSingleton(clusterConfig).getDownloadServers().entrySet()) {
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
     * 通过IP和StoreSocketPort端口删除store节点，ip+port能够确认唯一的store节点，并且一个store只能属于一个group。
     *
     * @param ip
     * @param storeSocketPort
     */
    public void removeStoreBySocketPort(String ip, int storeSocketPort) {
        for (Map.Entry<String, WeightedRoundRobinScheduling> weightedRoundRobinSchedulingEntry : HostResolverImpl.getSingleton(clusterConfig).getDownloadServers().entrySet()) {
            for (WeightedRoundRobinScheduling.Server server : weightedRoundRobinSchedulingEntry.getValue().healthilyServers) {
                if (server.getStoreInfo().getIp().equals(ip) && server.getStoreInfo().getStoreSocketPort() == storeSocketPort) {
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
        HostResolverImpl.getSingleton(clusterConfig).getDownloadServers().entrySet().stream().forEach(entry -> {
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
                        uploadServers.add(tmp);
                    } catch (CloneNotSupportedException e) {
                        new RuntimeException(e);
                    }
                });
            }
        });
        HostResolverImpl.getSingleton(clusterConfig).getUploadServers().healthilyServers = uploadServers;
        logger.debug("update servers of upload");
    }
}
