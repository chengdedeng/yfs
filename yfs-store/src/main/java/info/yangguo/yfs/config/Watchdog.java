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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Component
public class Watchdog {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private YfsConfig yfsConfig;

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
                    || (fileEvent.getRemoveNodes().size() == 0 && !fileEvent.getAddNodes().contains(clusterProperties.getLocal()))
                    || (fileEvent.getRemoveNodes().size() == 0 && !fileEvent.getMetaNodes().contains(clusterProperties.getLocal()))) {
                logger.info("Resync:\n{}", JsonUtil.toJson(fileEvent, true));
                yfsConfig.fileEventMap.replace(key, version, fileEvent);
            } else {
                if ((fileEvent.getRemoveNodes().size() == 0 && fileEvent.getAddNodes().contains(clusterProperties.getLocal())) && !FileService.checkExist(FileService.getPath(clusterProperties, key))) {
                    fileEvent.getAddNodes().remove(clusterProperties.getLocal());
                    logger.info("Resync:\n{}", JsonUtil.toJson(fileEvent, true));
                    yfsConfig.fileEventMap.replace(key, version, fileEvent);
                }
                if ((fileEvent.getRemoveNodes().size() == 0 && fileEvent.getMetaNodes().contains(clusterProperties.getLocal()) && !FileService.checkExist(FileService.makeMetadataPath(FileService.getPath(clusterProperties, key))))) {
                    fileEvent.getMetaNodes().remove(clusterProperties.getLocal());
                    logger.info("Resync:\n{}", JsonUtil.toJson(fileEvent, true));
                    yfsConfig.fileEventMap.replace(key, version, fileEvent);
                }
            }
        });
        logger.debug("file**************************watchdog");
    }

    /**
     * 定时扫描本地文件，修复数据。
     * 1. 检测checksum，修复损坏文件。
     * 2. 根据本地文件，修复元数据。
     */
    @Scheduled(initialDelayString = "${yfs.store.watchdog.initial_delay}", fixedDelayString = "${yfs.store.watchdog.repair_delay}")
    public void repair() {
        try {
            Files.walkFileTree(Paths.get(FileService.getPath(clusterProperties, "")), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!Files.isHidden(file)) {
                        String filePath = FileService.getRelativePath(clusterProperties, file.toString());
                        String originFilePath = FileService.makeFilePath(filePath);
                        try {
                            String checksum = String.valueOf(FileUtils.checksumCRC32(file.toFile()));
                            Versioned<FileEvent> fileEventVersioned = yfsConfig.fileEventMap.get(originFilePath);
                            if (fileEventVersioned != null) {
                                FileEvent fileEvent = fileEventVersioned.value();
                                if (FileService.verifyMetadataPath(filePath)) {
                                    if (!fileEvent.getMetaCrc32().equals(checksum)) {
                                        fileEvent.getMetaNodes().remove(clusterProperties.getLocal());
                                        yfsConfig.fileEventMap.replace(originFilePath, fileEventVersioned.version(), fileEvent);
                                    }
                                } else {
                                    if (!fileEvent.getFileCrc32().equals(checksum)) {
                                        fileEvent.getAddNodes().remove(clusterProperties.getLocal());
                                        yfsConfig.fileEventMap.replace(originFilePath, fileEventVersioned.version(), fileEvent);
                                    }
                                }
                            } else {
                                Versioned<RepairEvent> repairEventVersioned = yfsConfig.repairEventMap.get(filePath);
                                if (repairEventVersioned == null) {
                                    HashMap map = Maps.newHashMap();
                                    map.put(checksum, Sets.newHashSet(clusterProperties.getLocal()));
                                    RepairEvent repairEvent = new RepairEvent(new Date().getTime(), map);
                                    yfsConfig.repairEventMap.putIfAbsent(filePath, repairEvent);
                                } else {
                                    Set<String> existNodes = yfsConfig.makeRepairExistNode.apply(repairEventVersioned.value().getRepairInfo());
                                    if (!existNodes.contains(clusterProperties.getLocal())) {
                                        Set<String> nodes = repairEventVersioned.value().getRepairInfo().get(checksum);
                                        if (nodes == null) {
                                            nodes = new HashSet<>();
                                        }
                                        nodes.add(clusterProperties.getLocal());
                                        yfsConfig.repairEventMap.replace(filePath, repairEventVersioned.version(), repairEventVersioned.value());
                                    } else if (existNodes.size() == clusterProperties.getStore().getNode().size()) {
                                        if (new Date().getTime() - repairEventVersioned.value().getCreateTime() > clusterProperties.getStore().getWatchdog().getRepair_delay() * 3) {
                                            yfsConfig.repairEventMap.remove(filePath);
                                        } else {
                                            yfsConfig.repairEventMap.replace(filePath, repairEventVersioned.version(), repairEventVersioned.value());
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Repair {} failure", filePath, e);
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
     * 定时向Gateway上传存储节点信息，Gateway才能根据store的信息进行路由。
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
        } catch (IOException e) {
            logger.error("server check fail", e);
        }
        logger.debug("server**************************watchdog");
    }
}
