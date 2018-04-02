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

import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.po.ClusterProperties;
import info.yangguo.yfs.util.PropertiesUtil;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.messaging.Endpoint;
import io.atomix.primitive.Persistence;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterConfig {
    private final static String keyPrefix = "yfs.gateway.";
    public static ClusterProperties clusterProperties = new ClusterProperties();
    public static ConsistentMap storeInfoMap = null;

    static {
        Map<String, String> map = PropertiesUtil.getProperty("cluster.properties");
        Set<Integer> nodeIds = new HashSet<>();
        map.entrySet().stream().forEach(entry -> {
            String key = entry.getKey();
            String regex = ".*\\[(\\d)\\].*";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(key);
            if (matcher.matches()) {
                int index = Integer.valueOf(matcher.group(1));
                nodeIds.add(index);
            }
        });
        for (int i = 0; i < nodeIds.size(); i++) {
            if (!nodeIds.contains(i)) {
                throw new RuntimeException("node id need to correct!");
            }
        }
        for (int i = 0; i < nodeIds.size(); i++) {
            clusterProperties.getNode().add(new ClusterProperties.ClusterNode());
        }
        Arrays.stream(ClusterProperties.class.getDeclaredFields()).forEach(field1 -> {
            if (field1.getType().getName().endsWith("String")) {
                try {
                    field1.set(clusterProperties, map.get(keyPrefix + field1.getName()));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            if (field1.getType().getName().endsWith("List")) {
                try {
                    List<ClusterProperties.ClusterNode> clusterNodeList = (List<ClusterProperties.ClusterNode>) field1.get(clusterProperties);
                    for (int i = 0; i < clusterNodeList.size(); i++) {
                        for (Field field2 : ClusterProperties.ClusterNode.class.getFields()) {
                            if (field2.getType().getName().endsWith("String")) {
                                field2.set(clusterNodeList.get(i), map.get(keyPrefix + field1.getName() + "[" + i + "]." + field2.getName()));
                            } else if (field2.getType().getName().endsWith("int")) {
                                field2.set(clusterNodeList.get(i), Integer.valueOf(map.get(keyPrefix + field1.getName() + "[" + i + "]." + field2.getName())));
                            }
                        }
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public static void init() {
        Atomix.Builder builder = Atomix.builder();
        clusterProperties.getNode().stream().forEach(clusterNode -> {
            if (clusterNode.getId().equals(clusterProperties.getLocal())) {
                builder
                        .withLocalNode(Node.builder(clusterNode.getId())
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from(clusterNode.getHost(), clusterNode.getSocket_port()))
                                .build());
            }
        });

        builder.withBootstrapNodes(clusterProperties.getNode().parallelStream().map(clusterNode -> {
            return Node
                    .builder(clusterNode.getId())
                    .withType(Node.Type.DATA)
                    .withEndpoint(Endpoint.from(clusterNode.getHost(), clusterNode.getSocket_port())).build();
        }).collect(Collectors.toList()));
        File metadataDir = null;
        if (clusterProperties.getMetadataDir().startsWith("/")) {
            metadataDir = new File(clusterProperties.getMetadataDir() + "/" + clusterProperties.getLocal());
        } else {
            metadataDir = new File(FileUtils.getUserDirectoryPath(), clusterProperties.getMetadataDir() + "/" + clusterProperties.getLocal());
        }
        Atomix atomix = builder.withDataDirectory(metadataDir).build();
        atomix.start().join();
        Serializer serializer = Serializer.using(KryoNamespace.builder()
                .register(KryoNamespaces.BASIC)
                .register(Date.class)
                .register(StoreInfo.class)
                .build());

        storeInfoMap = atomix.<String, StoreInfo>consistentMapBuilder("store-info")
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(serializer)
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
    }
}
