package info.yangguo.yfs.service;

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.po.FileMetadata;
import io.atomix.core.Atomix;

public class MetadataService {
    public static void create(ClusterProperties clusterProperties, Atomix atomix, FileMetadata fileMetadata) {
        fileMetadata.setAddSourceNode(clusterProperties.getLocal());
        delete(clusterProperties, atomix, fileMetadata);
        YfsConfig.getConsistentMap(atomix).put(getId(fileMetadata), fileMetadata);
        YfsConfig.broadcastAddEvent(atomix, fileMetadata);
    }

    public static void delete(ClusterProperties clusterProperties, Atomix atomix, FileMetadata fileMetadata) {
        YfsConfig.getConsistentMap(atomix).remove(getId(fileMetadata));
    }


    public static String getId(String group, String partition, String name) {
        return group + "/" + partition + "/" + name;
    }

    public static String getId(FileMetadata fileMetadata) {
        return getId(fileMetadata.getGroup(), String.valueOf(fileMetadata.getPartition()), fileMetadata.getName());
    }

    public static FileMetadata getFileMetadata(Atomix atomix, String key) {
        return YfsConfig.getConsistentMap(atomix).get(key).value();
    }
}
