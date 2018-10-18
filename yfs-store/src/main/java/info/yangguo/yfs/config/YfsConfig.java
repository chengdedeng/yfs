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
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    public AtomicMap<String, FileMetadata> fileMetadataMap = null;
    public AtomicMap<String, StoreInfo> storeInfoMap = null;
    private HttpClient httpClient;
    public Cache<String, CountDownLatch> cache;
    @Autowired
    private ClusterProperties clusterProperties;

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
        if (properties.getStore().getMetadata().getDir().startsWith(File.separator)) {
            metadataDir = String.format(properties.getStore().getMetadata().getDir() + File.separator + "%s", properties.getLocal());
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + String.format(properties.getStore().getMetadata().getDir() + File.separator + "%s", properties.getLocal());
        }
        ManagedPartitionGroup managementGroup = RaftPartitionGroup.builder("system")
                .withMembers(ms.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
                .withNumPartitions(1)
                .withPartitionSize(ms.size())
                .withDataDirectory(new File(metadataDir + File.separator + "systme"))
                .build();


        return managementGroup;
    };

    private final Function<ClusterProperties, ManagedPartitionGroup> storeDataGroup = clusterProperties -> {
        List<Member> ms = storeMembers.apply(clusterProperties);

        String metadataDir = null;
        if (clusterProperties.getStore().getMetadata().getDir().startsWith(File.separator)) {
            metadataDir = String.format(clusterProperties.getStore().getMetadata().getDir() + File.separator + "%s", clusterProperties.getLocal());
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + String.format(clusterProperties.getStore().getMetadata().getDir() + File.separator + "%s", clusterProperties.getLocal());
        }

        ManagedPartitionGroup dataGroup = RaftPartitionGroup.builder("data")
                .withMembers(ms.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
                .withNumPartitions(7)
                .withPartitionSize(3)
                .withStorageLevel(StorageLevel.DISK)
                .withFlushOnCommit(false)
                .withDataDirectory(new File(metadataDir + File.separator + "data"))
                .build();

        return dataGroup;
    };

    public YfsConfig() {
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
        connectionManager.setMaxTotal(1000);
        connectionManager.setDefaultMaxPerRoute(1000);

        RequestConfig requestConfig = RequestConfig.custom()
                .build();

        this.httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionManager)
                .build();

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .removalListener(new RemovalListener() {
                    @Override
                    public void onRemoval(RemovalNotification notification) {
                        logger.debug("key:{} remove from cache", notification.getKey());
                    }
                })
                .build();
    }

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
        fileMetadataMap = atomix.<String, FileMetadata>atomicMapBuilder(CommonConstant.fileMetadataMapName)
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
                        syncFile(clusterProperties, fileMetadata1.getAddNodes(), fileMetadata1);
                        updateAddNodes(clusterProperties, fileMetadata1.getPath(), MapEvent.Type.INSERT.name());
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
                        syncFile(clusterProperties, addNodes, fileMetadata2);
                        updateAddNodes(clusterProperties, fileMetadata2.getPath(), MapEvent.Type.UPDATE.name());
                    } else if (removeNodes.size() > 0 && !removeNodes.contains(clusterProperties.getLocal())) {
                        FileMetadata fileMetadata3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileMetadata3, true),
                                JsonUtil.toJson(fileMetadata2, true));
                        FileService.delete(clusterProperties, fileMetadata2);
                        updateRemoveNodes(clusterProperties, fileMetadata2.getPath());
                    }
                    //为了实现QOS，yfs使用CountDownLatch来实现上传节点的有限时间阻塞，由yfs.store.qos_max_time配置决定。
                    //上传节点在上传之前会创建一个CountDownLatch放入到本地缓存，文件上传完成之后，上传节点自身会发布update event信息；
                    //同步节点会收到update event，然后同步文件，同步完成之后，也会发布update event；由于event是一个broadcast event，
                    //所以只有上传节点才需要处理latch，由于上传节点是addNodes中的第一个节点，所以便有下面的逻辑。
                    //特别注意新增节点为了防止网络导致的通知不可达，所以新增节点latch.countDown在写入元数据的时候就已经减一了，所以需要
                    //addNodes.size>1
                    if (removeNodes.size() == 0 && addNodes.size() > 1 && clusterProperties.getLocal().equals(addNodes.get(0))) {
                        CountDownLatch latch = cache.getIfPresent(fileMetadata2.getPath());
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
                .withAddress(clusterProperties.getGateway().getIp(), clusterProperties.getGateway().getPort())
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


    private void syncFile(ClusterProperties clusterProperties, List<String> addNodes, FileMetadata fileMetadata) {
        for (String addNode : addNodes) {
            ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(clusterProperties).get(addNode);
            String url = "http://" + clusterNode.getIp() + ":" + clusterNode.getHttp_port() + "/" + fileMetadata.getPath();
            HttpUriRequest httpUriRequest = new HttpGet(url);
            String id = fileMetadata.getPath();
            try {
                HttpResponse response = httpClient.execute(httpUriRequest);
                if (200 == response.getStatusLine().getStatusCode()) {
                    FileService.store(clusterProperties, fileMetadata, response);
                    logger.info("Sync Success:{}", id);
                    break;
                }
            } catch (Exception e) {
                logger.warn("Sync Failure:{}", id, e);
            }
        }
    }

    private boolean updateAddNodes(ClusterProperties clusterProperties, String key, String eventType) {
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

    private boolean updateRemoveNodes(ClusterProperties clusterProperties, String key) {
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
