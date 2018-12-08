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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.FileEvent;
import info.yangguo.yfs.common.po.RepairEvent;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.service.FileService;
import io.atomix.cluster.Member;
import io.atomix.core.Atomix;
import io.atomix.utils.time.Versioned;
import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class Watchdog {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private YfsConfig yfsConfig;
    @Autowired
    private Atomix storeAtomix;

    /**
     * 定时检查AtomicMap中的文件本地是否已经同步，出现不同步的情况有：
     * 1. 当前节点下线，再上线的时候，已经错过了部分文件的存储的event；
     * 2. 节点在线的情况下，也有可能出现event丢失的情况。
     */
    @Scheduled(initialDelayString = "${yfs.store.watchdog.initial_delay}", fixedDelayString = "${yfs.store.watchdog.fixed_delay}")
    public void watchFile() {
        yfsConfig.fileEventMap.entrySet().parallelStream().forEach(entry -> {
            String key = entry.getKey();
            FileEvent fileEvent = entry.getValue().value();
            long version = entry.getValue().version();
            if ((fileEvent.getRemoveNodes().size() > 0 && !fileEvent.getRemoveNodes().contains(clusterProperties.getLocal()))
                    || (fileEvent.getRemoveNodes().size() == 0 && !fileEvent.getAddNodes().contains(clusterProperties.getLocal()))) {
                logger.info("Resync:\n{}", JsonUtil.toJson(fileEvent, true));
                yfsConfig.fileEventMap.replace(key, version, fileEvent);
            } else {
                if ((fileEvent.getRemoveNodes().size() == 0 && fileEvent.getAddNodes().contains(clusterProperties.getLocal())) && !FileService.checkExist(FileService.getFullPath(clusterProperties, key))) {
                    fileEvent.getAddNodes().remove(clusterProperties.getLocal());
                    logger.info("Resync:\n{}", JsonUtil.toJson(fileEvent, true));
                    yfsConfig.fileEventMap.replace(key, version, fileEvent);
                }
            }
        });
        logger.debug("file**************************watchdog");
    }

    /**
     * 定时扫描本地文件，通过检测checksum，修复损坏文件。
     */
    @Scheduled(initialDelayString = "${yfs.store.watchdog.initial_delay}", fixedDelayString = "${yfs.store.watchdog.repair_delay}")
    public void repair() {
        try {
            Files.walkFileTree(Paths.get(FileService.getFullPath(clusterProperties, "")), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!Files.isHidden(file)) {
                        String relativePath = FileService.getRelativePath(clusterProperties, file.toString());
                        try {
                            String checksum = String.valueOf(FileUtils.checksumCRC32(file.toFile()));
                            Versioned<FileEvent> fileEventVersioned = yfsConfig.fileEventMap.get(relativePath);
                            if (fileEventVersioned != null) {
                                FileEvent fileEvent = fileEventVersioned.value();
                                if (!fileEvent.getFileCrc32().equals(checksum)) {
                                    fileEvent.getAddNodes().remove(clusterProperties.getLocal());
                                    yfsConfig.fileEventMap.replace(relativePath, fileEventVersioned.version(), fileEvent);
                                }
                            } else {
                                Versioned<RepairEvent> repairEventVersioned = yfsConfig.repairEventMap.get(relativePath);
                                if (repairEventVersioned == null) {
                                    HashMap map = Maps.newHashMap();
                                    map.put(checksum, Sets.newHashSet(clusterProperties.getLocal()));
                                    RepairEvent repairEvent = new RepairEvent(new Date().getTime(), map);
                                    yfsConfig.repairEventMap.putIfAbsent(relativePath, repairEvent);
                                } else {
                                    Set<String> existNodes = yfsConfig.makeRepairExistNode.apply(repairEventVersioned.value().getRepairInfo());
                                    if (!existNodes.contains(clusterProperties.getLocal())) {
                                        Set<String> nodes = repairEventVersioned.value().getRepairInfo().get(checksum);
                                        if (nodes == null) {
                                            nodes = new HashSet<>();
                                        }
                                        nodes.add(clusterProperties.getLocal());
                                        yfsConfig.repairEventMap.replace(relativePath, repairEventVersioned.version(), repairEventVersioned.value());
                                    } else if (existNodes.size() == clusterProperties.getStore().getNode().size()) {
                                        yfsConfig.repairEventMap.replace(relativePath, repairEventVersioned.version(), repairEventVersioned.value());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Repair {} failure", relativePath, e);
                        }
                    }
                    return super.visitFile(file, attrs);
                }
            });
        } catch (Exception e) {
            logger.error("Repair failure", e);
        }
        logger.debug("Repair**************************watchdog");
    }

    /**
     * 1.定时向Gateway上传存储节点信息，Gateway才能根据store的信息进行路由。
     * 2.定时更新本地node信息。
     */
    @Scheduled(fixedRate = 5000)
    public void watchServer() {
        StringBuilder fileDir = new StringBuilder();
        if (!clusterProperties.getStore().getFiledata().getDir().startsWith(File.separator)) {
            fileDir.append(FileUtils.getUserDirectoryPath()).append(File.separator);
        }
        String metadataDir = fileDir.toString() + clusterProperties.getStore().getMetadata().getDir();
        File metadataDirFile = new File(metadataDir);
        if (!metadataDirFile.exists()) {
            try {
                FileUtils.forceMkdir(metadataDirFile);
            } catch (IOException e) {
                logger.error("mkdir error", e);
            }
        }
        String fileDataDir = fileDir.toString() + clusterProperties.getStore().getFiledata().getDir();
        File fileDataDirFile = new File(fileDataDir);
        if (!fileDataDirFile.exists()) {
            try {
                FileUtils.forceMkdir(fileDataDirFile);
            } catch (IOException e) {
                logger.error("mkdir error", e);
            }
        }

        try {
            ClusterProperties.ClusterNode clusterNode = null;
            for (ClusterProperties.ClusterNode node : clusterProperties.getStore().getNode()) {
                if (node.getId().equals(clusterProperties.getLocal())) {
                    clusterNode = node;
                    break;
                }
            }
            StoreInfo storeInfo = new StoreInfo(
                    clusterProperties.getGroup(),
                    clusterNode.getId(),
                    clusterNode.getIp(),
                    clusterNode.getHttp_port(),
                    clusterNode.getSocket_port(),
                    clusterProperties.getGateway().getPort(),
                    new Date().getTime(),
                    FileSystemUtils.freeSpaceKb(metadataDir),
                    FileSystemUtils.freeSpaceKb(fileDataDir));


            yfsConfig.storeInfoMap.put(CommonConstant.storeInfoConsistentMapKey(clusterProperties.getGroup(), clusterNode.getIp(), clusterNode.getHttp_port()), storeInfo);
            logger.debug("Store[{}] has been updated to gateway finished", JsonUtil.toJson(storeInfo, false));
        } catch (IOException e) {
            logger.error("To update store of gateway fail", e);
        }
        //上线新节点之后，更新内存中的配置信息。
        try {
            Map<String, Member> members = storeAtomix.getMembershipService().getMembers().stream().collect(Collectors.toMap(member -> member.id().id(), member -> member));
            Map<String, ClusterProperties.ClusterNode> clusterNodeMap = yfsConfig.storeNodeMap.apply(clusterProperties);
            //集群节点数必须大于和等于本地配置节点数，才进行检测。也就是说集群一旦启动，在不停机的情况下，节点数只能等于和大于启动时的节点数。
            if (members.size() >= clusterNodeMap.size()) {
                //把新节点加入本地配置
                members
                        .entrySet()
                        .stream()
                        .filter(entry1 -> {
                            if (clusterNodeMap.containsKey(entry1.getKey())) {
                                return false;
                            } else {
                                return true;
                            }
                        })
                        .forEach(entry2 -> {
                            ClusterProperties.ClusterNode clusterNode = new ClusterProperties.ClusterNode();
                            clusterNode.setId(entry2.getKey());
                            clusterNode.setIp(entry2.getValue().address().host());
                            clusterNode.setSocket_port(entry2.getValue().address().port());
                            clusterNode.setHttp_port(Integer.valueOf((String) entry2.getValue().properties().get(CommonConstant.memberHttpPortPro)));
                            clusterProperties.getStore().getNode().add(clusterNode);
                            logger.info("Node[{}] has been added into the local config", JsonUtil.toJson(clusterNode, false));
                        });

                //把本地配置中过时节点剔除
                Iterator<ClusterProperties.ClusterNode> iterator = clusterProperties.getStore().getNode().iterator();
                while (iterator.hasNext()) {
                    ClusterProperties.ClusterNode node = iterator.next();
                    if (!members.containsKey(node.getId())) {
                        iterator.remove();
                        logger.warn("Node[{}] has been removed from the local config", JsonUtil.toJson(node, false));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Update local config fail", e);
        }
        logger.debug("server**************************watchdog");
    }
}
