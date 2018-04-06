package info.yangguo.yfs.common.po;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StoreInfo {
    private String group;
    private String nodeId;
    private String ip;
    private int storeHttpPort;
    private int storeSocketPort;
    private int gatewaySocketPort;
    private long updateTime;
    private Long metadataFreeSpaceKb;
    private Long fileFreeSpaceKb;

    private StoreInfo(){}

    public StoreInfo(String group, String nodeId, String ip, int storeHttpPort, int storeSocketPort, int gatewaySocketPort, long updateTime, Long metadataFreeSpaceKb, Long fileFreeSpaceKb) {
        this.group = group;
        this.nodeId = nodeId;
        this.ip = ip;
        this.storeHttpPort = storeHttpPort;
        this.storeSocketPort = storeSocketPort;
        this.gatewaySocketPort = gatewaySocketPort;
        this.updateTime = updateTime;
        this.metadataFreeSpaceKb = metadataFreeSpaceKb;
        this.fileFreeSpaceKb = fileFreeSpaceKb;
    }
}
