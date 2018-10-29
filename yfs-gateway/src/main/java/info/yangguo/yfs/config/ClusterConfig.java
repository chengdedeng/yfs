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

import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.util.PropertiesUtil;
import io.atomix.cluster.Member;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.map.AtomicMap;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClusterConfig {
    private Logger LOGGER = LoggerFactory.getLogger(ClusterConfig.class);
    public ClusterProperties clusterProperties = null;
    public Atomix atomix = null;
    public AtomicMap<String, StoreInfo> atomicMap = null;

    private static final Function<ClusterProperties, Map<String, ClusterProperties.ClusterNode>> gatewayNodeMap = properties -> properties.getNode().stream().collect(Collectors.toMap(node -> node.getId(), node -> node));
    private static final Function<ClusterProperties, List<Member>> gatewayMembers = properties -> {
        Map<String, ClusterProperties.ClusterNode> nodes = gatewayNodeMap.apply(properties);
        return nodes.entrySet().stream().map(node -> {
            return Member.builder()
                    .withId(node.getValue().getId())
                    .withAddress(node.getValue().getIp(), node.getValue().getSocket_port())
                    .build();
        }).collect(Collectors.toList());
    };
    private static final Function<ClusterProperties, Member> gatewayMember = properties -> {
        ClusterProperties.ClusterNode clusterNode = gatewayNodeMap.apply(properties).get(properties.getLocal());
        return Member.builder()
                .withId(properties.getLocal())
                .withAddress(clusterNode.getIp(), clusterNode.getSocket_port())
                .build();
    };
    private static final Function<ClusterProperties, ManagedPartitionGroup> gatewayManagementGroup = properties -> {
        List<Member> ms = gatewayMembers.apply(properties);
        String metadataDir = null;
        if (properties.getMetadataDir().startsWith(File.separator)) {
            metadataDir = properties.getMetadataDir();
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + properties.getMetadataDir();
        }
        ManagedPartitionGroup managementGroup = RaftPartitionGroup.builder("system")
                .withMembers(ms.stream().map(m -> m.id().id()).collect(Collectors.toSet()))
                .withNumPartitions(1)
                .withPartitionSize(ms.size())
                .withDataDirectory(new File(metadataDir + File.separator + "system"))
                .build();


        return managementGroup;
    };
    private static final Function<ClusterProperties, ManagedPartitionGroup> gatewayDataGroup = clusterProperties -> {
        List<Member> ms = gatewayMembers.apply(clusterProperties);

        String metadataDir = null;
        if (clusterProperties.getMetadataDir().startsWith(File.separator)) {
            metadataDir = clusterProperties.getMetadataDir();
        } else {
            metadataDir = FileUtils.getUserDirectoryPath() + File.separator + clusterProperties.getMetadataDir();
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


    protected ClusterProperties getClusterProperties() {
        String keyPrefix = "yfs.gateway.";
        ClusterProperties clusterProperties = new ClusterProperties();
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
            if (field1.getType().getName().endsWith("long")) {
                try {
                    field1.set(clusterProperties, Long.valueOf(map.get(keyPrefix + field1.getName())));
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
        return clusterProperties;
    }

    public ClusterConfig() {
        this.clusterProperties = getClusterProperties();
        Member m = gatewayMember.apply(clusterProperties);
        List<Member> ms = gatewayMembers.apply(clusterProperties);
        Atomix atomix = Atomix.builder()
                .withMemberId(clusterProperties.getLocal())
                .withAddress(m.address())
                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
                        .withNodes((Collection) ms)
                        .build())
                .withManagementGroup(gatewayManagementGroup.apply(clusterProperties))
                .withPartitionGroups(gatewayDataGroup.apply(clusterProperties))
                .withZone(CommonConstant.gatewayZone)
                .build();
        atomix.start().join();
        this.atomicMap = atomix.<String, StoreInfo>atomicMapBuilder(CommonConstant.storeInfoMapName)
                .withProtocol(MultiRaftProtocol.builder()
                        .withReadConsistency(ReadConsistency.LINEARIZABLE)
                        .build())
                .withSerializer(CommonConstant.protocolSerializer)
                .build();
        ;
        this.atomix = atomix;
        LOGGER.info("Atomix[{},{}]启动成功", m.id(), m.address().toString());
    }
}
