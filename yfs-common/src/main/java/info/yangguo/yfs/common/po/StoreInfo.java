package info.yangguo.yfs.common.po;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StoreInfo {
    private String group;
    private String nodeId;
    private String host;
    private int http_port;
    private long updateTime;
    private Long metadataFreeSpaceKb;
    private Long fileFreeSpaceKb;
}
