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
package info.yangguo.yfs.common;

import info.yangguo.yfs.common.po.FileEvent;
import info.yangguo.yfs.common.po.StoreInfo;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CommonConstant {
    public static final String fileMetadataMapName = "file-metadata";
    public static final String storeInfoMapName = "store-info";
    public static final String xHeaderPrefix = "x-yfs-";
    public static final String gatewayZone = "gateway";
    public static final String storeZone = "store";
    public static final String fileName = "x-filename";

    public static final Serializer protocolSerializer = Serializer.using(Namespace.builder()
            .register(ArrayList.class)
            .register(Date.class)
            .register(StoreInfo.class)
            .register(FileEvent.class)
            .register(Map.class)
            .register(HashMap.class)
            .build());

    /**
     * 构造Storeinfo ConsistentMap的key
     *
     * @param group
     * @param ip
     * @param http_port
     * @return
     */
    public static String storeInfoConsistentMapKey(String group, String ip, int http_port) {
        return new StringBuilder().append(group).append("-").append(ip).append("-").append(http_port).toString();
    }
}
