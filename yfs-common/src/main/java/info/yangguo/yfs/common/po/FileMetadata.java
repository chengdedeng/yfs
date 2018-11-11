package info.yangguo.yfs.common.po;

import lombok.*;

import java.util.Map;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileMetadata {
    /**
     * file original name
     */
    private String name;
    /**
     * file mime type
     */
    private String type;
    /**
     * file size
     */
    private String size;
    /**
     * store path
     */
    private String path;
    /**
     * hash by md5
     */
    private String md5;
    /**
     * extension metadata by user
     */
    private Map<String, String> extension;
}
