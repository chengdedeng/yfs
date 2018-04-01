package info.yangguo.yfs.po;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ServerMetadata {
    private long time;
    private long metadataFreeSpaceKb;
    private long fileFreeSpaceKb;
}
