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

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.po.FileMetadata;
import io.atomix.utils.time.Versioned;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MetadataService {
    public static boolean create(ClusterProperties clusterProperties, FileMetadata fileMetadata, int qos) throws InterruptedException {
        boolean result = false;
        fileMetadata.getAddNodes().add(clusterProperties.getLocal());
        CountDownLatch countDownLatch = new CountDownLatch(qos);
        YfsConfig.cache.put(getKey(fileMetadata), countDownLatch);
        YfsConfig.fileMetadataConsistentMap.put(getKey(fileMetadata), fileMetadata);
        //Preventing network traffic from failing
        countDownLatch.countDown();
        result = countDownLatch.await(clusterProperties.getStore().getQos_max_time(), TimeUnit.SECONDS);
        YfsConfig.cache.invalidate(getKey(fileMetadata));
        return result;
    }

    public static boolean softDelete(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        boolean result = false;
        Versioned<FileMetadata> tmp = YfsConfig.fileMetadataConsistentMap.get(getKey(fileMetadata));
        if (tmp != null) {
            long version = tmp.version();
            fileMetadata = tmp.value();
            fileMetadata.getRemoveNodes().add(clusterProperties.getLocal());
            result = YfsConfig.fileMetadataConsistentMap.replace(getKey(fileMetadata), version, fileMetadata);
        }
        return result;
    }


    public static String getKey(String group, Integer partition, String name) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append(group)
                .append("/")
                .append(partition)
                .append("/")
                .append(name);
        return stringBuilder.toString();
    }

    public static String getKey(FileMetadata fileMetadata) {
        return getKey(fileMetadata.getGroup(), fileMetadata.getPartition(), fileMetadata.getName());
    }
}
