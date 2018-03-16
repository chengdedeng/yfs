package info.yangguo.yfs.service;

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.po.FileMetadata;
import io.atomix.core.Atomix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetadataService {
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private Atomix atomix;

    public void create(FileMetadata fileMetadata) {
        fileMetadata.setAddSourceNode(clusterProperties.getLocal());
        YfsConfig.getConsistentMap(atomix).put(getId(fileMetadata), fileMetadata);
        YfsConfig.broadcastAddEvent(atomix, fileMetadata);
    }

    public void delete(FileMetadata fileMetadata) {
    }


    public static String getId(String group, String partition, String name) {
        return group + "/" + partition + "/" + name;
    }

    public static String getId(FileMetadata fileMetadata) {
        return getId(fileMetadata.getGroup(), String.valueOf(fileMetadata.getPartition()), fileMetadata.getName());
    }

    public FileMetadata getFileMetadata(String key) {
        return YfsConfig.getConsistentMap(atomix).get(key).value();
    }
}
