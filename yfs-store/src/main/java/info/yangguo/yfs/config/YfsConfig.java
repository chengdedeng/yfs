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
import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import io.atomix.cluster.Member;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.MapEvent;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties({ClusterProperties.class})
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    private static final String fileMetadataMapName = "file-metadata";
    public static AtomicMap<String, FileMetadata> fileMetadataMap = null;
    public static AtomicMap<String, StoreInfo> storeInfoMap = null;
    private static HttpClient httpClient;
    public static Cache<String, CountDownLatch> cache;

    static {
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

    private final Function<ClusterProperties, Map<String, ClusterProperties.ClusterNode>> storeNodeMap = properties -> properties.getStore().getNode().stream().collect(Collectors.toMap(node -> node.getId(), node -> node));
    private final Function<ClusterProperties, Map<String, ClusterProperties.ClusterNode>> gatewayNodeMap = properties -> properties.getGateway().getNode().stream().collect(Collectors.toMap(node -> node.getId(), node -> node));

    private final Function<ClusterProperties, List<Member>> storeMembers = properties -> {
        Map<String, ClusterProperties.ClusterNode> nodes = storeNodeMap.apply(properties);
        return nodes.entrySet().stream().map(node -> {
            return Member.builder()
                    .withId(node.getValue().getId())
                    .withAddress(node.getValue().getIp(), node.getValue().getSocket_port())
                    .build();
        }).collect(Collectors.toList());
    };

    private final Function<ClusterProperties, List<Member>> gatewayMembers = properties -> {
        Map<String, ClusterProperties.ClusterNode> nodes = gatewayNodeMap.apply(properties);
        return nodes.entrySet().stream().map(node -> {
            return Member.builder()
                    .withId(node.getValue().getId())
                    .withAddress(node.getValue().getIp(), node.getValue().getSocket_port())
                    .build();
        }).collect(Collectors.toList());
    };

    private final Function<ClusterProperties, Member> storeMember = properties -> {
        ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(properties).get(properties.getLocal());
        return Member.builder()
                .withId(properties.getLocal())
                .withAddress(clusterNode.getIp(), clusterNode.getSocket_port())
                .build();
    };

    private final Function<ClusterProperties, ManagedPartitionGroup> storeManagementGroup = properties -> {
        List<Member> ms = storeMembers.apply(properties);
        String metadataDir = null;
        if (properties.getStore().getMetadata().getDir().startsWith("/")) {
            metadataDir = String.format(properties.getStore().getMetadata().getDir() + "/%s", properties.getLocal());
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + "/" + String.format(properties.getStore().getMetadata().getDir() + "/%s", properties.getLocal());
        }
        ManagedPartitionGroup managementGroup = RaftPartitionGroup.builder("system")
                .withMembers(ms.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
                .withNumPartitions(1)
                .withPartitionSize(ms.size())
                .withDataDirectory(new File(metadataDir + "/systme"))
                .build();


        return managementGroup;
    };

    private final Function<ClusterProperties, ManagedPartitionGroup> storeDataGroup = clusterProperties -> {
        List<Member> ms = storeMembers.apply(clusterProperties);

        String metadataDir = null;
        if (clusterProperties.getStore().getMetadata().getDir().startsWith("/")) {
            metadataDir = String.format(clusterProperties.getStore().getMetadata().getDir() + "/%s", clusterProperties.getLocal());
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + "/" + String.format(clusterProperties.getStore().getMetadata().getDir() + "/%s", clusterProperties.getLocal());
        }

        ManagedPartitionGroup dataGroup = RaftPartitionGroup.builder("data")
                .withMembers(ms.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
                .withNumPartitions(7)
                .withPartitionSize(3)
                .withStorageLevel(StorageLevel.DISK)
                .withFlushOnCommit(false)
                .withDataDirectory(new File(metadataDir + "/data"))
                .build();

        return dataGroup;
    };

    @Autowired
    private ClusterProperties clusterProperties;

    @Bean(name = "storeAtomix")
    public Atomix getStoreAtomix() {
        Member m = storeMember.apply(clusterProperties);
        List<Member> ms = storeMembers.apply(clusterProperties);
        Atomix atomix = Atomix.builder()
                .withMemberId(m.id())
                .withAddress(m.address())
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes((Collection) ms)
                        .build())
                .withManagementGroup(storeManagementGroup.apply(clusterProperties))
                .withPartitionGroups(storeDataGroup.apply(clusterProperties))
                .build();
        atomix.start().join();
        fileMetadataMap = atomix.<String, FileMetadata>atomicMapBuilder(fileMetadataMapName)
                .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .withSerializer(CommonConstant.protocolSerializer)
                .build();
        fileMetadataMap.addListener(event -> {
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
        List<Member> ms = gatewayMembers.apply(clusterProperties);
        Atomix atomix = Atomix.builder()
                .withMemberId(clusterProperties.getLocal())
                .withAddress(clusterProperties.getGateway().getIp(),clusterProperties.getGateway().getPort())
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes((Collection) ms)
                        .build())
                .withProfiles(Profile.client())
                .build();

        atomix.start().join();
        storeInfoMap = atomix.<String, StoreInfo>atomicMapBuilder(CommonConstant.storeInfoMapName)
                .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .withSerializer(CommonConstant.protocolSerializer)
                .build();
        return atomix;
    }


    private long syncFile(ClusterProperties clusterProperties, List<String> addNodes, FileMetadata fileMetadata) {
        long checkSum = 0L;
        for (String addNode : addNodes) {
            ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(clusterProperties).get(addNode);
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
            Versioned<FileMetadata> tmp = fileMetadataMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            if (!fileMetadata.getAddNodes().contains(clusterProperties.getLocal())) {
                fileMetadata.getAddNodes().add(clusterProperties.getLocal());
            }
            result = fileMetadataMap.replace(key, version, fileMetadata);
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
            Versioned<FileMetadata> tmp = fileMetadataMap.get(key);
            long version = tmp.version();
            fileMetadata = tmp.value();
            if (!fileMetadata.getRemoveNodes().contains(clusterProperties.getLocal())) {
                fileMetadata.getRemoveNodes().add(clusterProperties.getLocal());
            }
            if (fileMetadata.getRemoveNodes().size() == clusterProperties.getStore().getNode().size()) {
                fileMetadataMap.remove(key);
                result = true;
            } else {
                result = fileMetadataMap.replace(key, version, fileMetadata);
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
