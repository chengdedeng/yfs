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
}
