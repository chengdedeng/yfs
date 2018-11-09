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
import info.yangguo.yfs.common.po.FileEvent;
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
    public AtomicMap<String, FileEvent> fileEventMap = null;
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
            metadataDir = properties.getStore().getMetadata().getDir();
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + properties.getStore().getMetadata().getDir();
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
            metadataDir = clusterProperties.getStore().getMetadata().getDir();
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + clusterProperties.getStore().getMetadata().getDir();
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
        fileEventMap = atomix.<String, FileEvent>atomicMapBuilder(CommonConstant.fileMetadataMapName)
                .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .withSerializer(CommonConstant.protocolSerializer)
                .build();
        fileEventMap.addListener(event -> {
            switch (event.type()) {
                case INSERT:
                    Versioned<FileEvent> tmp1 = event.newValue();
                    FileEvent fileEvent1 = tmp1.value();
                    if (!fileEvent1.getAddNodes().contains(clusterProperties.getLocal())) {
                        logger.info("{} Event Info:\n{}",
                                MapEvent.Type.INSERT.name(),
                                JsonUtil.toJson(fileEvent1, true));
                        syncFile(clusterProperties, fileEvent1, MapEvent.Type.INSERT);
                    }
                case UPDATE:
                    Versioned<FileEvent> tmp2 = event.newValue();
                    Versioned<FileEvent> tmp3 = event.oldValue();
                    FileEvent fileEvent2 = tmp2.value();
                    List<String> addNodes = fileEvent2.getAddNodes();
                    List<String> removeNodes = fileEvent2.getRemoveNodes();
                    if (tmp3 != null && removeNodes.size() == 0
                            && !addNodes.contains(clusterProperties.getLocal())) {
                        FileEvent fileEvent3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileEvent3, true),
                                JsonUtil.toJson(fileEvent2, true));
                        syncFile(clusterProperties, fileEvent2, MapEvent.Type.UPDATE);
                    } else if (removeNodes.size() > 0 && !removeNodes.contains(clusterProperties.getLocal())) {
                        FileEvent fileEvent3 = tmp3.value();
                        logger.info("{} Event Info:\nOldValue:{}\nNewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                JsonUtil.toJson(fileEvent3, true),
                                JsonUtil.toJson(fileEvent2, true));
                        FileService.delete(clusterProperties, fileEvent2.getPath());
                        updateRemoveNodes(clusterProperties, fileEvent2.getPath());
                    }
                    //为了实现QOS，yfs使用CountDownLatch来实现上传节点的有限时间阻塞，由yfs.store.qos_max_time配置决定。
                    //上传节点在上传之前会创建一个CountDownLatch放入到本地缓存，文件上传完成之后，上传节点自身会发布update event信息；
                    //同步节点会收到update event，然后同步文件，同步完成之后，也会发布update event；由于event是一个broadcast event，
                    //所以只有上传节点才需要处理latch，由于上传节点是addNodes中的第一个节点，所以便有下面的逻辑。
                    //特别注意新增节点为了防止网络导致的通知不可达，所以新增节点latch.countDown在写入元数据的时候就已经减一了，所以需要
                    //addNodes.size>1
                    if (removeNodes.size() == 0 && addNodes.size() > 1 && clusterProperties.getLocal().equals(addNodes.get(0))) {
                        CountDownLatch latch = cache.getIfPresent(fileEvent2.getPath());
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
                .withZone(CommonConstant.storeZone)
                .withRack(clusterProperties.getGroup())
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

    /**
     * 从别的store同步文件到当前store
     *
     * @param clusterProperties
     * @param fileEvent
     * @param type
     */
    private void syncFile(ClusterProperties clusterProperties, FileEvent fileEvent, MapEvent.Type type) {
        for (String addNode : fileEvent.getAddNodes()) {
            ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(clusterProperties).get(addNode);
            String url = "http://" + clusterNode.getIp() + ":" + clusterNode.getHttp_port() + "/" + fileEvent.getPath();
            HttpUriRequest httpUriRequest = new HttpGet(url);
            String id = fileEvent.getPath();
            try {
                HttpResponse response = httpClient.execute(httpUriRequest);
                if (200 == response.getStatusLine().getStatusCode())
                    FileService.store(clusterProperties, id, response);
                Versioned<FileEvent> tmp = fileEventMap.get(fileEvent.getPath());
                long version = tmp.version();
                fileEvent = tmp.value();
                if (!fileEvent.getAddNodes().contains(clusterProperties.getLocal()))
                    fileEvent.getAddNodes().add(clusterProperties.getLocal());
                fileEventMap.replace(fileEvent.getPath(), version, fileEvent);
                logger.info("Sync {} Success", id);
                break;
            } catch (Exception e) {
                logger.warn("Sync {} Failure", id, e);
            }
        }
    }

    private boolean updateRemoveNodes(ClusterProperties clusterProperties, String key) {
        boolean result = false;
        FileEvent fileEvent = null;
        try {
            Versioned<FileEvent> tmp = fileEventMap.get(key);
            long version = tmp.version();
            fileEvent = tmp.value();
            if (!fileEvent.getRemoveNodes().contains(clusterProperties.getLocal())) {
                fileEvent.getRemoveNodes().add(clusterProperties.getLocal());
            }
            if (fileEvent.getRemoveNodes().size() == clusterProperties.getStore().getNode().size()) {
                fileEventMap.remove(key);
                result = true;
            } else {
                result = fileEventMap.replace(key, version, fileEvent);
            }
        } catch (Exception e) {
            logger.warn("UpdateRemoveNodes Failure:{}", key, e);
        }
        if (result == true) {
            logger.info("UpdateRemoveNodes Info:{}", JsonUtil.toJson(fileEvent, true));
            logger.info("UpdateRemoveNodes Success:{}", key);
        }
        return result;
    }
}
