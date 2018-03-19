package info.yangguo.yfs.service;

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.po.FileMetadata;
import io.atomix.core.Atomix;
import io.atomix.utils.time.Versioned;
import org.apache.commons.lang3.SerializationUtils;

public class MetadataService {
    public static void create(ClusterProperties clusterProperties, Atomix atomix, FileMetadata fileMetadata) {
        fileMetadata.setAddSourceNode(clusterProperties.getLocal());
        YfsConfig.getConsistentMap(atomix).put(getId(fileMetadata), fileMetadata);
    }

    public static void softDelete(ClusterProperties clusterProperties, Atomix atomix, FileMetadata fileMetadata) {
        Versioned<FileMetadata> tmp = YfsConfig.getConsistentMap(atomix).get(getId(fileMetadata));
        if (tmp != null) {
            long version = tmp.version();
            fileMetadata = SerializationUtils.clone(tmp.value());
            fileMetadata.setRemoveSourceNode(clusterProperties.getLocal());
            YfsConfig.getConsistentMap(atomix).replace(getId(fileMetadata), version, fileMetadata);
            YfsConfig.broadcastDeleteEvent(atomix, fileMetadata);
        }
    }

    public static void delete(Atomix atomix, FileMetadata fileMetadata) {
        YfsConfig.getConsistentMap(atomix).remove(getId(fileMetadata));
    }


    public static String getId(String group, String partition, String name) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append(group)
                .append("/")
                .append(partition)
                .append("/")
                .append(name);
        return stringBuilder.toString();
    }

    public static String getId(FileMetadata fileMetadata) {
        return getId(fileMetadata.getGroup(), String.valueOf(fileMetadata.getPartition()), fileMetadata.getName());
    }

    public static FileMetadata getFileMetadata(Atomix atomix, FileMetadata fileMetadata) {
        String key = getId(fileMetadata);
        Versioned<FileMetadata> tmp = YfsConfig.getConsistentMap(atomix).get(key);
        fileMetadata = SerializationUtils.clone(tmp.value());
        return fileMetadata;
    }
}
