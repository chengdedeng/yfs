package info.yangguo.yfs.config;

import com.google.common.collect.Maps;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.messaging.Endpoint;
import io.atomix.primitive.Persistence;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
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
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties({ClusterProperties.class})
public class YfsConfig {
    private static Logger logger = LoggerFactory.getLogger(YfsConfig.class);
    private static final String mapName = "file-metadata";
    private static final String addTopicName = "add-event";
    private static final String deleteTopicName = "delete-event";
    public static Serializer serializer = null;
    private static volatile ConsistentMap<String, FileMetadata> consistentMap = null;
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

        atomix.messagingService().<FileMetadata, FileMetadata>subscribe(addTopicName, serializer::decode, message -> {
            if (!clusterProperties.getLocal().equals(message.getAddSourceNode())) {
                ClusterProperties.ClusterNode clusterNode = mapAtomicReference.get().get(message.getAddSourceNode());
                String url = "http://" + clusterNode.getHost() + ":" + clusterNode.getHttp_port() + "/" + MetadataService.getId(message);
                HttpUriRequest httpUriRequest = new HttpGet(url);
                try {
                    logger.info(url);
                    HttpResponse response = httpClient.execute(httpUriRequest);
                    long checkSum = FileService.store(clusterProperties, message, response);
                    if (checkSum == message.getCheckSum()) {
                        for (int i = 0; i < clusterProperties.getNode().size(); i++) {
                            try {
                                Thread.sleep(RandomUtils.nextInt(i, i * 1000 + 10));
                            } catch (InterruptedException e) {
                                logger.warn(e.toString());
                            }
                            if (updateAddTarget(clusterProperties, atomix, MetadataService.getId(message))) {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("同步失败:{}", JsonUtil.toJson(message, true), e);
                }
            }
            return CompletableFuture.completedFuture(message);
        }, serializer::encode).join();

        atomix.messagingService().<FileMetadata, FileMetadata>subscribe(deleteTopicName, serializer::decode, message -> {
            FileService.delete(clusterProperties, message);
            if (!clusterProperties.getLocal().equals(message.getRemoveSourceNode())) {
                for (int i = 0; i < clusterProperties.getNode().size(); i++) {
                    try {
                        Thread.sleep(RandomUtils.nextInt(i, i * 1000 + 10));
                    } catch (InterruptedException e) {
                        logger.warn("随机暂停失败", e);
                    }
                    if (updateRemoveTarget(clusterProperties, atomix, MetadataService.getId(message))) {
                        break;
                    }
                }
            }
            return CompletableFuture.completedFuture(message);
        }, serializer::encode).join();
        return atomix;
    }

    public static ConsistentMap<String, FileMetadata> getConsistentMap(Atomix atomix) {
        if (consistentMap == null) {
            synchronized (YfsConfig.class) {
                if (consistentMap == null) {
                    consistentMap = atomix.<String, FileMetadata>consistentMapBuilder(mapName)
                            .withPersistence(Persistence.PERSISTENT)
                            .withSerializer(serializer)
                            .withBackups(2)
                            .withRetryDelay(Duration.ofSeconds(1))
                            .withMaxRetries(3)
                            .build();
                }
            }
        }
        return consistentMap;
    }

    public static void broadcastAddEvent(Atomix atomix, FileMetadata fileMetadata) {
        atomix.messagingService().broadcast(addTopicName, fileMetadata, serializer::encode);
    }

    public static void broadcastDeleteEvent(Atomix atomix, FileMetadata fileMetadata) {
        atomix.messagingService().broadcast(deleteTopicName, fileMetadata, serializer::encode);
    }

    private static boolean updateAddTarget(ClusterProperties clusterProperties, Atomix atomix, String key) {
        try {
            Versioned<FileMetadata> tmp = getConsistentMap(atomix).get(key);
            long version = tmp.version();
            FileMetadata fileMetadata = SerializationUtils.clone(tmp.value());
            fileMetadata.getAddTargetNodes().add(clusterProperties.getLocal());
            boolean result = getConsistentMap(atomix).replace(key, version, fileMetadata);
            if (result == true) {
                logger.info("updateAddTarget:{}", JsonUtil.toJson(fileMetadata, true));
            }
            return result;
        } catch (Exception e) {
            logger.warn(e.toString());
            return false;
        }
    }

    private static boolean updateRemoveTarget(ClusterProperties clusterProperties, Atomix atomix, String key) {
        try {
            Versioned<FileMetadata> tmp = getConsistentMap(atomix).get(key);
            long version = tmp.version();
            FileMetadata fileMetadata = SerializationUtils.clone(tmp.value());
            fileMetadata.getRemoveTargetNodes().add(clusterProperties.getLocal());
            if (fileMetadata.getRemoveTargetNodes().size() == clusterProperties.getNode().size() - 1) {
                getConsistentMap(atomix).remove(key);
                logger.info("updateRemoveTarget:{}", JsonUtil.toJson(fileMetadata, true));
                return true;
            } else {
                boolean result = getConsistentMap(atomix).replace(key, version, fileMetadata);
                if (result == true) {
                    logger.info("updateRemoveTarget:{}", JsonUtil.toJson(fileMetadata, true));
                }
                return result;
            }
        } catch (Exception e) {
            return false;
        }
    }
}
