package info.yangguo.yfs.common;

import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.common.po.ServerMetadata;
import info.yangguo.yfs.common.po.StoreInfo;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Date;

public class CommonConstant {
    public static final String storeInfoMapName = "store-info";

    public static final Serializer protocolSerializer = Serializer.using(Namespace.builder()
            .register(ArrayList.class)
            .register(Date.class)
            .register(StoreInfo.class)
            .register(FileMetadata.class)
            .register(ServerMetadata.class)
            .build());

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
