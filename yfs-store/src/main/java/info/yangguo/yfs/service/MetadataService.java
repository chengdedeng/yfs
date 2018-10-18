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
package info.yangguo.yfs.service;

import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import io.atomix.utils.time.Versioned;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MetadataService {
    public static boolean create(ClusterProperties clusterProperties, YfsConfig yfsConfig, FileMetadata fileMetadata, int qos) throws InterruptedException {
        boolean result = false;
        fileMetadata.getAddNodes().add(clusterProperties.getLocal());
        CountDownLatch countDownLatch = new CountDownLatch(qos);
        yfsConfig.cache.put(fileMetadata.getPath(), countDownLatch);
        yfsConfig.fileMetadataMap.put(fileMetadata.getPath(), fileMetadata);
        //Preventing network traffic from failing
        countDownLatch.countDown();
        result = countDownLatch.await(clusterProperties.getStore().getQos_max_time(), TimeUnit.SECONDS);
        yfsConfig.cache.invalidate(fileMetadata.getPath());
        return result;
    }

    public static boolean softDelete(ClusterProperties clusterProperties, YfsConfig yfsConfig, FileMetadata fileMetadata) {
        boolean result = false;
        Versioned<FileMetadata> tmp = yfsConfig.fileMetadataMap.get(fileMetadata.getPath());
        if (tmp != null) {
            long version = tmp.version();
            fileMetadata = tmp.value();
            fileMetadata.getRemoveNodes().add(clusterProperties.getLocal());
            result = yfsConfig.fileMetadataMap.replace(fileMetadata.getPath(), version, fileMetadata);
        }
        return result;
    }
}
