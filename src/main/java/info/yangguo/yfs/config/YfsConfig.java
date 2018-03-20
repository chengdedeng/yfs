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
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.MapEvent;
import io.atomix.messaging.Endpoint;
import io.atomix.primitive.Persistence;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties({ClusterProperties.class})
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    private static final String mapName = "file-metadata";
    public static Serializer serializer = null;
    public static ConsistentMap<String, FileMetadata> consistentMap = null;
    private static HttpClient httpClient;

    static {
        serializer = Serializer.using(KryoNamespace.builder()
                .register(KryoNamespaces.BASIC)
                .register(Date.class)
                .register(FileMetadata.class)
                .build());

        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
        connectionManager.setMaxTotal(1000);
        connectionManager.setDefaultMaxPerRoute(1000);

        RequestConfig requestConfig = RequestConfig.custom()
                .build();

        httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionManager)
                .build();
    }

    @Autowired
    private ClusterProperties clusterProperties;

    @Bean
    public Atomix getAtomix() {
        AtomicReference<Map<String, ClusterProperties.ClusterNode>> mapAtomicReference = new AtomicReference<>(Maps.newHashMap());
        Atomix.Builder builder = Atomix.builder();
        clusterProperties.getNode().stream().forEach(clusterNode -> {
            mapAtomicReference.get().put(clusterNode.getId(), clusterNode);
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
        if (clusterProperties.getMetadata().getDir().startsWith("/")) {
            metadataDir = new File(clusterProperties.getMetadata().getDir() + "/" + clusterProperties.getLocal());
        } else {
            metadataDir = new File(FileUtils.getUserDirectoryPath(), clusterProperties.getMetadata().getDir() + "/" + clusterProperties.getLocal());
        }
        Atomix atomix = builder.withDataDirectory(metadataDir).build();
        atomix.start().join();
        consistentMap = atomix.<String, FileMetadata>consistentMapBuilder(mapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(serializer)
                .withBackups(2)
                .build();
        consistentMap.addListener(event -> {
            switch (event.type()) {
                case INSERT:
                    Versioned<FileMetadata> tmp1 = event.newValue();
                    FileMetadata fileMetadata1 = tmp1.value();
                    if (!clusterProperties.getLocal().equals(fileMetadata1.getAddSourceNode())) {
                        logger.info("{} Event Info:\n{}",
                                MapEvent.Type.INSERT.name(),
                                JsonUtil.toJson(fileMetadata1, true));
                        ClusterProperties.ClusterNode clusterNode = mapAtomicReference.get().get(fileMetadata1.getAddSourceNode());
                        long checkSum = syncFile(clusterProperties, clusterNode, fileMetadata1);
                        if (fileMetadata1.getCheckSum() == checkSum) {
                            updateAddTarget(clusterProperties, MetadataService.getKey(fileMetadata1), MapEvent.Type.INSERT.name());
                        }
                    }
                case UPDATE:
                    Versioned<FileMetadata> tmp2 = event.newValue();
                    Versioned<FileMetadata> tmp3 = event.oldValue();
                    FileMetadata fileMetadata2 = tmp2.value();
                    String addSourceNode = fileMetadata2.getAddSourceNode();
                    Set<String> addTargetNodes = fileMetadata2.getAddTargetNodes();
                    String removeSourceNode = fileMetadata2.getRemoveSourceNode();
                    Set<String> removeTargetNodes = fileMetadata2.getRemoveTargetNodes();
                    if (tmp3 != null && removeSourceNode == null
                            && !clusterProperties.getLocal().equals(addSourceNode)
                            && !addTargetNodes.contains(clusterProperties.getLocal())) {
                        FileMetadata fileMetadata3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileMetadata3, true),
                                JsonUtil.toJson(fileMetadata2, true));
                        if (fileMetadata2.getCheckSum() != FileService.checkFile(clusterProperties, fileMetadata2)) {
                            ClusterProperties.ClusterNode clusterNode = mapAtomicReference.get().get(fileMetadata2.getAddSourceNode());
                            long checkSum = syncFile(clusterProperties, clusterNode, fileMetadata2);
                            if (fileMetadata2.getCheckSum() == checkSum) {
                                updateAddTarget(clusterProperties, MetadataService.getKey(fileMetadata2), MapEvent.Type.UPDATE.name());
                            }
                        } else {
                            updateAddTarget(clusterProperties, MetadataService.getKey(fileMetadata2), MapEvent.Type.UPDATE.name());
                        }
                    } else if (tmp3 != null && removeSourceNode != null && !clusterProperties.getLocal().equals(removeSourceNode) && !removeTargetNodes.contains(clusterProperties.getLocal())) {
                        FileMetadata fileMetadata3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileMetadata3, false),
                                JsonUtil.toJson(fileMetadata2, false));
                        String key = MetadataService.getKey(fileMetadata2);
                        FileService.delete(clusterProperties, fileMetadata2);
                        updateRemoveTarget(clusterProperties, key);
                    }
            }
        });
        return atomix;
    }

    private static long syncFile(ClusterProperties clusterProperties, ClusterProperties.ClusterNode clusterNode, FileMetadata fileMetadata) {
        String url = "http://" + clusterNode.getHost() + ":" + clusterNode.getHttp_port() + "/" + MetadataService.getKey(fileMetadata);
        HttpUriRequest httpUriRequest = new HttpGet(url);
        String id = MetadataService.getKey(fileMetadata);
        long checkSum = 0L;
        try {
            HttpResponse response = httpClient.execute(httpUriRequest);
            checkSum = FileService.store(clusterProperties, fileMetadata, response);
            logger.info("Sync Success:{}", id);
        } catch (Exception e) {
            logger.warn("Sync Failure:{}", id, e);
        }
        return checkSum;
    }

    private static boolean updateAddTarget(ClusterProperties clusterProperties, String key, String eventType) {
        boolean result = false;
        FileMetadata fileMetadata = null;
        try {
            Versioned<FileMetadata> tmp = consistentMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            fileMetadata.getAddTargetNodes().add(clusterProperties.getLocal());
            result = consistentMap.replace(key, version, fileMetadata);
        } catch (Exception e) {
            logger.warn("{} UpdateAddTarget Failure:{}", eventType, key, e);
        }
        if (result == true) {
            logger.info("{} UpdateAddTarget Info:{}", eventType, JsonUtil.toJson(fileMetadata, true));
            logger.info("{} UpdateAddTarget Success:{}", eventType, key);
        }
        return result;
    }

    private static boolean updateRemoveTarget(ClusterProperties clusterProperties, String key) {
        boolean result = false;
        FileMetadata fileMetadata = null;
        try {
            Versioned<FileMetadata> tmp = consistentMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            fileMetadata.getRemoveTargetNodes().add(clusterProperties.getLocal());
            if (fileMetadata.getRemoveTargetNodes().size() == clusterProperties.getNode().size() - 1) {
                consistentMap.remove(key);
                result = true;
            } else {
                result = consistentMap.replace(key, version, fileMetadata);
            }
        } catch (Exception e) {
            logger.warn("UpdateRemoveTarget Failure:{}", key, e);
        }
        if (result == true) {
            logger.info("UpdateRemoveTarget Info:{}", JsonUtil.toJson(fileMetadata, true));
            logger.info("UpdateRemoveTarget Success:{}", key);
        }
        return result;
    }
}
