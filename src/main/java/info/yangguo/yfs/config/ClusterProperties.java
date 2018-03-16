package info.yangguo.yfs.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "yfs")
@Getter
@Setter
public class ClusterProperties {
    private String group;
    private String local;
    private Metadata metadata;
    private Filedata filedata;
    private List<ClusterNode> node;

    @Getter
    @Setter
    public static class ClusterNode {
        private String id;
        private String host;
        private int socket_port;
        private int http_port;
    }

    @Getter
    @Setter
    public static class Metadata {
        private String dir;
    }

    @Getter
    @Setter
    public static class Filedata {
        private String dir;
        private int partition;
    }
}
