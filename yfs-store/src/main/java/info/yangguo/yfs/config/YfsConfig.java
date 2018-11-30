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
import info.yangguo.yfs.common.po.RepairEvent;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.service.FileService;
import io.atomix.cluster.Member;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.MapEvent;
import io.atomix.core.profile.Profile;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.time.Versioned;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    public AtomicMap<String, FileEvent> fileEventMap = null;
    public AtomicMap<String, StoreInfo> storeInfoMap = null;
    public AtomicMap<String, RepairEvent> repairEventMap = null;
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

    public final Function<HashMap<String, HashSet<String>>, Set<String>> makeRepairExistNode = map -> map.entrySet().stream().map(entry -> entry.getValue()).flatMap(set -> set.stream()).collect(Collectors.toSet());

    private final Function<AtomicMapEvent<String, RepairEvent>, RepairEvent> makeReapirEvent = atomicMapEvent -> {
        String key = atomicMapEvent.key();
        Versioned<RepairEvent> newValue = atomicMapEvent.newValue();
        HashMap<String, HashSet<String>> newEvent = newValue.value().getRepairInfo();
        Set<String> existNodes = makeRepairExistNode.apply(newEvent);
        if (!existNodes.contains(clusterProperties.getLocal())) {
            String fullPath = FileService.getPath(clusterProperties, key);
            if (FileService.checkExist(fullPath)) {
                try {
                    String crc32 = String.valueOf(FileUtils.checksumCRC32(new File(fullPath)));
                    if (newEvent.containsKey(crc32)) {
                        newEvent.get(crc32).add(clusterProperties.getLocal());
                    } else {
                        HashSet<String> newNodes = new HashSet<>();
                        newNodes.add(clusterProperties.getLocal());
                        newEvent.put(crc32, newNodes);
                    }
                } catch (Exception e) {
                    if (newEvent.containsKey(null)) {
                        newEvent.get(null).add(clusterProperties.getLocal());
                    } else {
                        HashSet<String> newNodes = new HashSet<>();
                        newNodes.add(clusterProperties.getLocal());
                        newEvent.put(null, newNodes);
                    }
                }
            } else {
                if (newEvent.containsKey(null)) {
                    newEvent.get(null).add(clusterProperties.getLocal());
                } else {
                    HashSet<String> newNodes = new HashSet<>();
                    newNodes.add(clusterProperties.getLocal());
                    newEvent.put(null, newNodes);
                }
            }
        }
        return newValue.value();
    };

    public YfsConfig() {
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
            String key = event.key();
            switch (event.type()) {
                case INSERT:
                    Versioned<FileEvent> insertValue = event.newValue();
                    FileEvent insertEvent = insertValue.value();
                    if (!insertEvent.getAddNodes().contains(clusterProperties.getLocal())
                            || !insertEvent.getMetaNodes().contains(clusterProperties.getLocal())) {
                        logger.info("{} {}:\n{}",
                                MapEvent.Type.INSERT.name(),
                                key,
                                JsonUtil.toJson(insertEvent, true));
                        syncFile(clusterProperties, key, insertValue, MapEvent.Type.INSERT);
                    }
                    break;
                case UPDATE:
                    Versioned<FileEvent> newValue = event.newValue();
                    Versioned<FileEvent> oldValue = event.oldValue();
                    FileEvent newFileEvent = newValue.value();
                    FileEvent oldFileEvent = oldValue.value();
                    List<String> addNodes = newFileEvent.getAddNodes();
                    List<String> metaNodes = newFileEvent.getMetaNodes();
                    List<String> removeNodes = newFileEvent.getRemoveNodes();
                    if (removeNodes.size() == 0
                            && (!addNodes.contains(clusterProperties.getLocal()) || !metaNodes.contains(clusterProperties.getLocal()))) {
                        logger.info("{} {}:\noldValue:{}\nnewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                key,
                                JsonUtil.toJson(oldFileEvent, true),
                                JsonUtil.toJson(newFileEvent, true));
                        syncFile(clusterProperties, key, newValue, MapEvent.Type.UPDATE);
                    } else if (removeNodes.size() > 0 && !removeNodes.contains(clusterProperties.getLocal())) {
                        logger.info("{} {}:\noldValue:{}\nnewValue:{}",
                                MapEvent.Type.UPDATE.name(),
                                key,
                                JsonUtil.toJson(oldFileEvent, true),
                                JsonUtil.toJson(newFileEvent, true));
                        FileService.delete(clusterProperties, key);
                        updateRemoveNodes(clusterProperties, key);
                    }
                    //为了实现QOS，yfs使用CountDownLatch来实现上传节点的有限时间阻塞，由yfs.store.qos_max_time配置决定。
                    //上传节点在上传之前会创建一个CountDownLatch放入到本地缓存，文件上传完成之后，上传节点自身会发布update event信息；
                    //同步节点会收到update event，然后同步文件，同步完成之后，也会发布update event；由于event是一个broadcast event，
                    //所以只有上传节点才需要处理latch，由于上传节点是addNodes中的第一个节点，所以便有下面的逻辑。
                    //特别注意新增节点为了防止网络导致的通知不可达，所以新增节点latch.countDown在写入元数据的时候就已经减一了，所以需要
                    //addNodes.size>1
                    if (removeNodes.size() == 0
                            && addNodes.size() > 1
                            && clusterProperties.getLocal().equals(addNodes.get(0))
                            && metaNodes.size() > 1
                            && clusterProperties.getLocal().equals(metaNodes.get(0))
                            && addNodes.size() == metaNodes.size()
                    ) {
                        CountDownLatch latch = cache.getIfPresent(event.key());
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
                    break;
                default:
                    break;
            }
        });


        repairEventMap = atomix.<String, RepairEvent>atomicMapBuilder(CommonConstant.repairMetadataMapName)
                .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .withSerializer(CommonConstant.protocolSerializer)
                .build();
        repairEventMap.addListener(event -> {
            String key = event.key();
            switch (event.type()) {
                case INSERT:
                    Versioned<RepairEvent> versionedInsertEventNewValue = event.newValue();
                    HashMap<String, HashSet<String>> insertEventValue = versionedInsertEventNewValue.value().getRepairInfo();
                    Set<String> insertExistNodes = makeRepairExistNode.apply(insertEventValue);
                    if (!insertExistNodes.contains(clusterProperties.getLocal())) {
                        repairEventMap.replace(key, versionedInsertEventNewValue.version(), makeReapirEvent.apply(event));
                    }
                    break;
                case UPDATE:
                    Versioned<RepairEvent> versionedUpdateEventNewValue = event.newValue();
                    Set<String> updateExistNodes = makeRepairExistNode.apply(versionedUpdateEventNewValue.value().getRepairInfo());
                    if (!updateExistNodes.contains(clusterProperties.getLocal())) {
                        repairEventMap.replace(key, versionedUpdateEventNewValue.version(), makeReapirEvent.apply(event));
                    } else {
                        //所有节点投票完成之后，才能判定checksum。由于并发，所以要先判断relationkey是否已经删除。
                        if (updateExistNodes.size() == clusterProperties.getStore().getNode().size()) {
                            String relationKey = null;
                            Versioned<RepairEvent> versionedRelationRepairValue = null;
                            Pair<String, Versioned<RepairEvent>> fileRepairInfo = null;
                            Pair<String, Versioned<RepairEvent>> metaRepairInfo = null;

                            //必须file和metadata都投票完成，才能修复。
                            if (FileService.verifyMetadataPath(key)) {
                                relationKey = FileService.makeFilePath(key);
                                versionedRelationRepairValue = repairEventMap.get(relationKey);
                                fileRepairInfo = new ImmutablePair<>(relationKey, versionedRelationRepairValue);
                                metaRepairInfo = new ImmutablePair<>(key, versionedUpdateEventNewValue);
                            } else {
                                relationKey = FileService.makeMetadataPath(key);
                                versionedRelationRepairValue = repairEventMap.get(relationKey);
                                fileRepairInfo = new ImmutablePair<>(key, versionedUpdateEventNewValue);
                                metaRepairInfo = new ImmutablePair<>(relationKey, versionedRelationRepairValue);
                            }
                            if (fileRepairInfo.getRight() != null && metaRepairInfo.getRight() != null) {
                                Set<String> existNodes2 = makeRepairExistNode.apply(versionedRelationRepairValue.value().getRepairInfo());
                                if (existNodes2.size() == clusterProperties.getStore().getNode().size()) {
                                    Optional<ImmutablePair<String, Integer>> fileChecksum = fileRepairInfo.getRight().value().getRepairInfo().entrySet().stream()
                                            .filter(entry -> {
                                                if (entry.getKey() == null) {
                                                    return false;
                                                } else {
                                                    return true;
                                                }
                                            })
                                            .map(entry -> new ImmutablePair<String, Integer>(entry.getKey(), entry.getValue().size()))
                                            .max(Comparator.comparing(Pair::getValue));
                                    Optional<ImmutablePair<String, Integer>> metaChecksum = metaRepairInfo.getRight().value().getRepairInfo().entrySet().stream()
                                            .filter(entry -> {
                                                if (entry.getKey() == null) {
                                                    return false;
                                                } else {
                                                    return true;
                                                }
                                            })
                                            .map(entry -> new ImmutablePair<String, Integer>(entry.getKey(), entry.getValue().size()))
                                            .max(Comparator.comparing(Pair::getValue));

                                    if (fileChecksum.isPresent() && metaChecksum.isPresent()) {
                                        FileEvent fileEvent = new FileEvent();
                                        fileEvent.setFileCrc32(fileChecksum.get().getLeft());
                                        fileEvent.getAddNodes().addAll(fileRepairInfo.getRight().value().getRepairInfo().get(fileChecksum.get().getLeft()));
                                        fileEvent.setMetaCrc32(metaChecksum.get().getLeft());
                                        fileEvent.getMetaNodes().addAll(metaRepairInfo.getRight().value().getRepairInfo().get(metaChecksum.get().getLeft()));
                                        if (fileEventMap.putIfAbsent(fileRepairInfo.getLeft(), fileEvent) == null) {
                                            repairEventMap.remove(fileRepairInfo.getLeft());
                                            repairEventMap.remove(metaRepairInfo.getLeft());
                                            logger.warn("Repair {} success", fileRepairInfo.getLeft());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    break;
                case REMOVE:
                    String fileKey = FileService.makeFilePath(key);
                    if (!fileEventMap.containsKey(fileKey)) {
                        FileService.delete(clusterProperties, fileKey);
                        logger.warn("Repair {} failure", fileKey);
                    }
                    break;
                default:
                    break;
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
     * @param fileEventVersioned
     * @param type
     */
    private void syncFile(ClusterProperties clusterProperties, String fileRelativePath, Versioned<FileEvent> fileEventVersioned, MapEvent.Type type) {
        boolean isSendEvent = false;
        FileEvent fileEvent = fileEventVersioned.value();
        String metaRelativePath = FileService.makeMetadataPath(fileRelativePath);
        if (fileEvent.getFileCrc32() != null && !fileEvent.getAddNodes().contains(clusterProperties.getLocal())) {
            for (String addNode : fileEvent.getAddNodes()) {
                try {
                    ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(clusterProperties).get(addNode);
                    String fileUrl = "http://" + clusterNode.getIp() + ":" + clusterNode.getHttp_port() + "/" + fileRelativePath;

                    FileService.store(clusterProperties, fileRelativePath, fileUrl, fileEvent.getFileCrc32());
                    fileEvent.getAddNodes().add(clusterProperties.getLocal());
                    isSendEvent = true;
                    break;
                } catch (Exception e) {
                    FileService.deleteFile(clusterProperties, fileRelativePath);
                    logger.warn("Failed to sync {}", fileRelativePath, e);
                }
            }
        }

        if (fileEvent.getMetaCrc32() != null && !fileEvent.getMetaNodes().contains(clusterProperties.getLocal())) {
            for (String metaNode : fileEvent.getMetaNodes()) {
                try {
                    ClusterProperties.ClusterNode clusterNode = storeNodeMap.apply(clusterProperties).get(metaNode);
                    String fileUrl = "http://" + clusterNode.getIp() + ":" + clusterNode.getHttp_port() + "/" + metaRelativePath;

                    FileService.store(clusterProperties, metaRelativePath, fileUrl, fileEvent.getMetaCrc32());
                    fileEvent.getMetaNodes().add(clusterProperties.getLocal());
                    isSendEvent = true;
                    break;
                } catch (Exception e) {
                    FileService.deleteMeta(clusterProperties, fileRelativePath);
                    logger.warn("Failed to sync {}", metaRelativePath, e);
                }
            }
        }

        if (isSendEvent)
            if (fileEventMap.replace(fileRelativePath, fileEventVersioned.version(), fileEvent))
                logger.debug("Success to replace event when sync file {}", fileRelativePath);

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
