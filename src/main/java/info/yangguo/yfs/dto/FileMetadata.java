package info.yangguo.yfs.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;

@Getter
@Setter
public class FileMetadata {
    private Date beginDate;
    private Date endDate;
    private long fileSize;
    private String fileName;
    private List<FileIterm> fileItermList;

    @Getter
    @Setter
    public static class FileIterm {

    }
}
