package info.yangguo.yfs.po;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

@Getter
@Setter
public class FileMetadata implements Serializable {
    private String name;
    private long size;
    private String group;
    private int partition;
    private Date createTime;
    private long checkSum;
    private String addSourceNode;
    private Set<String> addTargetNodes;
    private String removeSourceNode;
    private Set<String> removeTargetNodes;
}
