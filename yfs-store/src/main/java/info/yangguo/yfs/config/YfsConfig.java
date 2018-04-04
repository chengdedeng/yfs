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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.po.ServerMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.MapEvent;
import io.atomix.messaging.Endpoint;
import io.atomix.primitive.Persistence;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties({ClusterProperties.class})
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    private static final String fileMetadataMapName = "file-metadata";
    private static Serializer serializer = null;
    public static ConsistentTreeMap<FileMetadata> fileMetadataConsistentMap = null;
    public static ConsistentMap<String, StoreInfo> storeInfoConsistentMap = null;
    private static HttpClient httpClient;
    private static Map<String, ClusterProperties.ClusterNode> clusterNodeMap = new HashMap<>();
    public static Cache<String, CountDownLatch> cache;

    static {
        CommonConstant.kryoBuilder.register(FileMetadata.class).register(ServerMetadata.class);
        serializer = Serializer.using(CommonConstant.kryoBuilder.build());

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

        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(300, TimeUnit.SECONDS)
                .removalListener(new RemovalListener() {
                    @Override
                    public void onRemoval(RemovalNotification notification) {
                        logger.debug("key:{} remove from cache", notification.getKey());
                    }
                })
                .build();
    }

    @Autowired
    private ClusterProperties clusterProperties;

    @Bean(name = "storeAtomix")
    public Atomix getStoreAtomix() {
        Atomix.Builder builder = Atomix.builder();
        clusterProperties.getStore().getNode().stream().forEach(clusterNode -> {
            clusterNodeMap.put(clusterNode.getId(), clusterNode);
            if (clusterNode.getId().equals(clusterProperties.getLocal())) {
                builder
                        .withLocalNode(Node.builder(clusterNode.getId())
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from(clusterNode.getIp(), clusterNode.getSocket_port()))
                                .build());
            }
        });

        builder.withBootstrapNodes(clusterProperties.getStore().getNode().parallelStream().map(clusterNode -> {
            return Node
                    .builder(clusterNode.getId())
                    .withType(Node.Type.DATA)
                    .withEndpoint(Endpoint.from(clusterNode.getIp(), clusterNode.getSocket_port())).build();
        }).collect(Collectors.toList()));
        File metadataDir = null;
        if (clusterProperties.getStore().getMetadata().getDir().startsWith("/")) {
            metadataDir = new File(clusterProperties.getStore().getMetadata().getDir() + "/" + clusterProperties.getLocal());
        } else {
            metadataDir = new File(FileUtils.getUserDirectoryPath(), clusterProperties.getStore().getMetadata().getDir() + "/" + clusterProperties.getLocal());
        }
        Atomix atomix = builder.withDataDirectory(metadataDir).build();
        atomix.start().join();
        fileMetadataConsistentMap = atomix.<FileMetadata>consistentTreeMapBuilder(fileMetadataMapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(serializer)
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
        fileMetadataConsistentMap.addListener(event -> {
            switch (event.type()) {
                case INSERT:
                    Versioned<FileMetadata> tmp1 = event.newValue();
                    FileMetadata fileMetadata1 = tmp1.value();
                    if (!fileMetadata1.getAddNodes().contains(clusterProperties.getLocal())) {
                        logger.info("{} Event Info:\n{}",
                                MapEvent.Type.INSERT.name(),
                                JsonUtil.toJson(fileMetadata1, true));
                        long checkSum = syncFile(clusterProperties, fileMetadata1.getAddNodes(), fileMetadata1);
                        if (fileMetadata1.getCheckSum() == checkSum) {
                            updateAddNodes(clusterProperties, MetadataService.getKey(fileMetadata1), MapEvent.Type.INSERT.name());
                        }
                    }
                case UPDATE:
                    Versioned<FileMetadata> tmp2 = event.newValue();
                    Versioned<FileMetadata> tmp3 = event.oldValue();
                    FileMetadata fileMetadata2 = tmp2.value();
                    List<String> addNodes = fileMetadata2.getAddNodes();
                    List<String> removeNodes = fileMetadata2.getRemoveNodes();
                    if (tmp3 != null && removeNodes.size() == 0
                            && !addNodes.contains(clusterProperties.getLocal())) {
                        FileMetadata fileMetadata3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileMetadata3, true),
                                JsonUtil.toJson(fileMetadata2, true));
                        if (fileMetadata2.getCheckSum() != FileService.checkFile(clusterProperties, fileMetadata2)) {
                            long checkSum = syncFile(clusterProperties, addNodes, fileMetadata2);
                            if (fileMetadata2.getCheckSum() == checkSum) {
                                updateAddNodes(clusterProperties, MetadataService.getKey(fileMetadata2), MapEvent.Type.UPDATE.name());
                            }
                        } else {
                            updateAddNodes(clusterProperties, MetadataService.getKey(fileMetadata2), MapEvent.Type.UPDATE.name());
                        }
                    } else if (removeNodes.size() > 0 && !removeNodes.contains(clusterProperties.getLocal())) {
                        FileMetadata fileMetadata3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileMetadata3, true),
                                JsonUtil.toJson(fileMetadata2, true));
                        String key = MetadataService.getKey(fileMetadata2);
                        FileService.delete(clusterProperties, fileMetadata2);
                        updateRemoveNodes(clusterProperties, key);
                    }

                    if (removeNodes.size() == 0 && addNodes.size() > 1 && clusterProperties.getLocal().equals(addNodes.get(0))) {
                        CountDownLatch latch = cache.getIfPresent(MetadataService.getKey(fileMetadata2));
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
            }
        });
        return atomix;
    }

    @Bean(name = "gatewayAtomix")
    public Atomix getGatewayAtomix() {
        Atomix.Builder builder = Atomix.builder().withLocalNode(Node.builder(clusterProperties.getLocal())
                .withType(Node.Type.CLIENT)
                .withEndpoint(Endpoint.from(clusterProperties.getGateway().getIp(), clusterProperties.getGateway().getPort()))
                .build());


        builder.withBootstrapNodes(clusterProperties.getGateway().getNode().parallelStream().map(clusterNode -> {
            return Node
                    .builder(clusterNode.getId())
                    .withType(Node.Type.DATA)
                    .withEndpoint(Endpoint.from(clusterNode.getIp(), clusterNode.getSocket_port())).build();
        }).collect(Collectors.toList()));

        Atomix atomix = builder.build();
        atomix.start().join();
        storeInfoConsistentMap = atomix.<String, StoreInfo>consistentMapBuilder(CommonConstant.storeInfoMapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(serializer)
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
        return atomix;
    }


    private static long syncFile(ClusterProperties clusterProperties, List<String> addNodes, FileMetadata fileMetadata) {
        long checkSum = 0L;
        for (String addNode : addNodes) {
            ClusterProperties.ClusterNode clusterNode = clusterNodeMap.get(addNode);
            String url = "http://" + clusterNode.getIp() + ":" + clusterNode.getHttp_port() + "/" + MetadataService.getKey(fileMetadata);
            HttpUriRequest httpUriRequest = new HttpGet(url);
            String id = MetadataService.getKey(fileMetadata);
            try {
                HttpResponse response = httpClient.execute(httpUriRequest);
                if (200 == response.getStatusLine().getStatusCode()) {
                    checkSum = FileService.store(clusterProperties, fileMetadata, response);
                    logger.info("Sync Success:{}", id);
                    break;
                }
            } catch (Exception e) {
                logger.warn("Sync Failure:{}", id, e);
            }
        }
        return checkSum;
    }

    private static boolean updateAddNodes(ClusterProperties clusterProperties, String key, String eventType) {
        boolean result = false;
        FileMetadata fileMetadata = null;
        try {
            Versioned<FileMetadata> tmp = fileMetadataConsistentMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            if (!fileMetadata.getAddNodes().contains(clusterProperties.getLocal())) {
                fileMetadata.getAddNodes().add(clusterProperties.getLocal());
            }
            result = fileMetadataConsistentMap.replace(key, version, fileMetadata);
        } catch (Exception e) {
            logger.warn("{} UpdateAddNodes Failure:{}", eventType, key, e);
        }
        if (result == true) {
            logger.info("{} UpdateAddNodes Info:{}", eventType, JsonUtil.toJson(fileMetadata, true));
            logger.info("{} UpdateAddNodes Success:{}", eventType, key);
        }
        return result;
    }

    private static boolean updateRemoveNodes(ClusterProperties clusterProperties, String key) {
        boolean result = false;
        FileMetadata fileMetadata = null;
        try {
            Versioned<FileMetadata> tmp = fileMetadataConsistentMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            if (!fileMetadata.getRemoveNodes().contains(clusterProperties.getLocal())) {
                fileMetadata.getRemoveNodes().add(clusterProperties.getLocal());
            }
            if (fileMetadata.getRemoveNodes().size() == clusterProperties.getStore().getNode().size()) {
                fileMetadataConsistentMap.remove(key);
                result = true;
            } else {
                result = fileMetadataConsistentMap.replace(key, version, fileMetadata);
            }
        } catch (Exception e) {
            logger.warn("UpdateRemoveNodes Failure:{}", key, e);
        }
        if (result == true) {
            logger.info("UpdateRemoveNodes Info:{}", JsonUtil.toJson(fileMetadata, true));
            logger.info("UpdateRemoveNodes Success:{}", key);
        }
        return result;
    }
}
