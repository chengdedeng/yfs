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

public class MetadataService {
    public static void create(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        fileMetadata.getAddNodes().add(clusterProperties.getLocal());
        YfsConfig.consistentMap.put(getKey(fileMetadata), fileMetadata);
    }

    public static boolean softDelete(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        boolean result = false;
        Versioned<FileMetadata> tmp = YfsConfig.consistentMap.get(getKey(fileMetadata));
        if (tmp != null) {
            long version = tmp.version();
            fileMetadata = tmp.value();
            fileMetadata.getRemoveNodes().add(clusterProperties.getLocal());
            result = YfsConfig.consistentMap.replace(getKey(fileMetadata), version, fileMetadata);
        }
        return result;
    }


    public static String getKey(String group, String partition, String name) {
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
        return getKey(fileMetadata.getGroup(), String.valueOf(fileMetadata.getPartition()), fileMetadata.getName());
    }
}
