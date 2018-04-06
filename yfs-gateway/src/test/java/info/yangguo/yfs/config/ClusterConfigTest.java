package info.yangguo.yfs.config;

import info.yangguo.yfs.po.ClusterProperties;
import org.junit.Assert;
import org.junit.Test;

public class ClusterConfigTest {
    @Test
    public void test() {
        ClusterProperties clusterProperties = ClusterConfig.getClusterProperties();
        Assert.assertEquals("server1", clusterProperties.getLocal());
        Assert.assertEquals("yfs-gateway/data/metadata", clusterProperties.getMetadataDir());
        Assert.assertEquals(6003, clusterProperties.getNode().get(2).getSocket_port());

    }
}
