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

import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
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
import java.util.Collection;
import java.util.Date;

@Component
public class Watchdog {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private YfsConfig yfsConfig;

    @Scheduled(initialDelayString = "${yfs.store.watchdog.initial_delay}", fixedDelayString = "${yfs.store.watchdog.fixed_delay}")
    public void watchFile() {
        Collection<Versioned<FileMetadata>> metadata = yfsConfig.fileMetadataMap.values();
        metadata.parallelStream().forEach(fileMetadataVersioned -> {
            FileMetadata fileMetadata = fileMetadataVersioned.value();
            long version = fileMetadataVersioned.version();
            if ((fileMetadata.getRemoveNodes().size() > 0 && !fileMetadata.getRemoveNodes().contains(clusterProperties.getLocal()))
                    || (fileMetadata.getRemoveNodes().size() == 0 && !fileMetadata.getAddNodes().contains(clusterProperties.getLocal()))) {
                logger.info("Resync:\n{}", JsonUtil.toJson(fileMetadata, true));
                yfsConfig.fileMetadataMap.replace(fileMetadata.getPath(), version, fileMetadata);
            }
        });
        logger.debug("file**************************watchdog");
    }

    @Scheduled(fixedRate = 5000)
    public void watchServer() {
        StringBuilder fileDir = new StringBuilder();
        if (!clusterProperties.getStore().getFiledata().getDir().startsWith("/")) {
            fileDir.append(FileUtils.getUserDirectoryPath()).append("/");
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
