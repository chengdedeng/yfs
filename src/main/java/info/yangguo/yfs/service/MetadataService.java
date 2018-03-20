package info.yangguo.yfs.service;

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.po.FileMetadata;
import io.atomix.utils.time.Versioned;
import org.apache.commons.lang3.SerializationUtils;

public class MetadataService {
    public static void create(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        fileMetadata.setAddSourceNode(clusterProperties.getLocal());
        YfsConfig.consistentMap.put(getKey(fileMetadata), fileMetadata);
    }

    public static void softDelete(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        Versioned<FileMetadata> tmp = YfsConfig.consistentMap.get(getKey(fileMetadata));
        if (tmp != null) {
            long version = tmp.version();
            fileMetadata = SerializationUtils.clone(tmp.value());
            fileMetadata.setRemoveSourceNode(clusterProperties.getLocal());
            YfsConfig.consistentMap.replace(getKey(fileMetadata), version, fileMetadata);
        }
    }

    public static void delete(FileMetadata fileMetadata) {
        YfsConfig.consistentMap.remove(getKey(fileMetadata));
    }


    public static String getKey(String group, String partition, String name) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append(group)
                .append("/")
                .append(partition)
                .append("/")
                .append(name);
        return stringBuilder.toString();
    }

    public static String getKey(FileMetadata fileMetadata) {
        return getKey(fileMetadata.getGroup(), String.valueOf(fileMetadata.getPartition()), fileMetadata.getName());
    }

    public static FileMetadata getFileMetadata(FileMetadata fileMetadata) {
        String key = getKey(fileMetadata);
        Versioned<FileMetadata> tmp = YfsConfig.consistentMap.get(key);
        fileMetadata = SerializationUtils.clone(tmp.value());
        return fileMetadata;
    }
}
