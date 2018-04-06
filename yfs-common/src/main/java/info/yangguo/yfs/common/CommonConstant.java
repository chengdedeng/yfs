package info.yangguo.yfs.common;

import info.yangguo.yfs.common.po.StoreInfo;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

import java.util.Date;

public class CommonConstant {
    public static final String storeInfoMapName = "store-info";
    public static final KryoNamespace.Builder kryoBuilder = KryoNamespace.builder()
            .register(KryoNamespaces.BASIC)
            .register(Date.class)
            .register(StoreInfo.class);

    /**
     * 构造Storeinfo ConsistentMap的key
     *
     * @param group
     * @param ip
     * @param http_port
     * @return
     */
    public static String storeInfoConsistentMapKey(String group, String ip, int http_port) {
        return new StringBuilder().append(group).append("-").append(ip).append("-").append(http_port).toString();
    }
}
