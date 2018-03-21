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

import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Component
public class Watchdog {
    private static Logger logger = LoggerFactory.getLogger(Watchdog.class);
    @Autowired
    private ClusterProperties clusterProperties;

    @Scheduled(initialDelayString = "${yfs.watchdog.initial_delay}", fixedDelayString = "${yfs.watchdog.fixed_delay}")
    public void watch() {
        logger.info("watchdog**************************");
        Collection<Versioned<FileMetadata>> metadata = YfsConfig.consistentMap.values();
        metadata.parallelStream().forEach(fileMetadataVersioned -> {
            FileMetadata fileMetadata = fileMetadataVersioned.value();
            if (new Date().getTime() - fileMetadata.getCreateTime().getTime() > 1000 * 60) {
                Set<String> addSet = new HashSet<>();
                addSet.add(fileMetadata.getAddSourceNode());
                addSet.addAll(fileMetadata.getAddTargetNodes());
                Set<String> removeSet = new HashSet<>();
                if (fileMetadata.getRemoveSourceNode() != null) {
                    removeSet.add(fileMetadata.getRemoveSourceNode());
                }
                removeSet.addAll(fileMetadata.getRemoveTargetNodes());
                String key = MetadataService.getKey(fileMetadata);
                long version = fileMetadataVersioned.version();
                if ((removeSet.size() > 0 && !removeSet.contains(clusterProperties.getLocal()))
                        || (removeSet.size() == 0 && !addSet.contains(clusterProperties.getLocal()))) {
                    logger.info("Resyn:\n{}", JsonUtil.toJson(fileMetadata, true));
                    YfsConfig.consistentMap.replace(key, version, fileMetadata);
                }
            }
        });
        logger.info("**************************watchdog");
    }
}
