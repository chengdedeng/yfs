package info.yangguo.yfs.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "atomix.cluster")
@Getter
@Setter
public class ClusterProperties {
    private String local;
    private List<ClusterNode> node;

    @Getter
    @Setter
    public static class ClusterNode {
        private String id;
        private String host;
        private int port;
    }
}
