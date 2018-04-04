package info.yangguo.yfs;

import info.yangguo.yfs.common.CommonConstant;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.impl.BlockingConsistentMap;
import io.atomix.messaging.Endpoint;
import io.atomix.primitive.Persistence;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;
import net.jodah.concurrentunit.Waiter;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Cluster2Test {
    private static class Task implements Runnable {
        private String serverName;
        private ConsistentMap storeInfoMap;

        public Task(String serverName, ConsistentMap storeInfoMap) {
            this.serverName = serverName;
            this.storeInfoMap = storeInfoMap;
        }

        @Override
        public void run() {
            Versioned<String> versioned = storeInfoMap.get("test");
            if (versioned != null)
                System.out.println("server:" + serverName + ",value:" + versioned.value());
        }
    }

    @Test
    public void run1() {
        Waiter waiter = new Waiter();
        Atomix.Builder builder = Atomix.builder();
        builder.withLocalNode(Node.builder("server1")
                .withType(Node.Type.DATA)
                .withEndpoint(Endpoint.from("localhost", 5001))
                .build()).withDataDirectory(new File("/Users/guo/yfs-gateway/data/server1"));
        builder.withBootstrapNodes(
                Node.builder("server1")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5001))
                        .build(),
                Node.builder("server2")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5002))
                        .build(),
                Node.builder("server3")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5003))
                        .build());
        Atomix atomix = builder.build();
        atomix.start().join();
        ConsistentMap<String, String> storeInfoMap = atomix.<String, String>consistentMapBuilder(CommonConstant.storeInfoMapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(Serializer.using(CommonConstant.kryoBuilder.build()))
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(new Cluster2Test.Task("server1", storeInfoMap), 0, 10, TimeUnit.SECONDS);
        try {
            waiter.await(1, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run2() {
        try {
            Waiter waiter = new Waiter();
            Atomix.Builder builder = Atomix.builder();
            builder.withLocalNode(Node.builder("server2")
                    .withType(Node.Type.DATA)
                    .withEndpoint(Endpoint.from("localhost", 5002))
                    .build()).withDataDirectory(new File("/Users/guo/yfs-gateway/data/server2"));
            builder.withBootstrapNodes(
                    Node.builder("server1")
                            .withType(Node.Type.DATA)
                            .withEndpoint(Endpoint.from("localhost", 5001))
                            .build(),
                    Node.builder("server2")
                            .withType(Node.Type.DATA)
                            .withEndpoint(Endpoint.from("localhost", 5002))
                            .build(),
                    Node.builder("server3")
                            .withType(Node.Type.DATA)
                            .withEndpoint(Endpoint.from("localhost", 5003))
                            .build());
            Atomix atomix = builder.build();
            atomix.start().join();
            ConsistentMap<String, String> storeInfoMap = atomix.<String, String>consistentMapBuilder(CommonConstant.storeInfoMapName)
                    .withPersistence(Persistence.PERSISTENT)
                    .withSerializer(Serializer.using(CommonConstant.kryoBuilder.build()))
                    .withRetryDelay(Duration.ofSeconds(1))
                    .withMaxRetries(3)
                    .withBackups(2)
                    .build();
            ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
            scheduledThreadPoolExecutor.scheduleAtFixedRate(new Cluster2Test.Task("server2", storeInfoMap), 0, 10, TimeUnit.SECONDS);

            waiter.await(1, TimeUnit.DAYS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run3() {
        Waiter waiter = new Waiter();
        Atomix.Builder builder = Atomix.builder();
        builder.withLocalNode(Node.builder("server3")
                .withType(Node.Type.DATA)
                .withEndpoint(Endpoint.from("localhost", 5003))
                .build()).withDataDirectory(new File("/Users/guo/yfs-gateway/data/server3"));
        builder.withBootstrapNodes(
                Node.builder("server1")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5001))
                        .build(),
                Node.builder("server2")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5002))
                        .build(),
                Node.builder("server3")
                        .withType(Node.Type.DATA)
                        .withEndpoint(Endpoint.from("localhost", 5003))
                        .build());
        Atomix atomix = builder.build();
        atomix.start().join();
        ConsistentMap<String, String> storeInfoMap = atomix.<String, String>consistentMapBuilder(CommonConstant.storeInfoMapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(Serializer.using(CommonConstant.kryoBuilder.build()))
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(new Cluster2Test.Task("server3", storeInfoMap), 0, 10, TimeUnit.SECONDS);
        try {
            waiter.await(1, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void client() throws InterruptedException, ExecutionException, TimeoutException {
        Waiter waiter = new Waiter();
        Atomix atomix = Atomix.builder()
                .withLocalNode(Node.builder("client")
                        .withType(Node.Type.CLIENT)
                        .withEndpoint(Endpoint.from("localhost", 5004))
                        .build())
                .withBootstrapNodes(
                        Node.builder("server1")
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from("localhost", 5001))
                                .build(),
                        Node.builder("server2")
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from("localhost", 5002))
                                .build(),
                        Node.builder("server3")
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from("localhost", 5003))
                                .build())
                .build();

        atomix.start().join();
        BlockingConsistentMap<String, String> storeInfoMap = (BlockingConsistentMap<String, String>) atomix.<String, String>consistentMapBuilder(CommonConstant.storeInfoMapName)
                .withPersistence(Persistence.PERSISTENT)
                .withSerializer(Serializer.using(CommonConstant.kryoBuilder.build()))
                .withRetryDelay(Duration.ofSeconds(1))
                .withMaxRetries(3)
                .withBackups(2)
                .build();
        Versioned<String> versioned = storeInfoMap.async().get("test").get();
        storeInfoMap.put("test", String.valueOf(RandomUtils.nextInt()));
        try {
            waiter.await(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
