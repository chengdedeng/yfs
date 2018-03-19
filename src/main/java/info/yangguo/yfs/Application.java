package info.yangguo.yfs;

import info.yangguo.yfs.config.ClusterProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ClusterProperties.class)
public class Application implements EmbeddedServletContainerCustomizer {
    @Autowired
    private ClusterProperties clusterProperties;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void customize(ConfigurableEmbeddedServletContainer configurableEmbeddedServletContainer) {
        clusterProperties.getNode().stream().forEach(clusterNode -> {
            if (clusterNode.getId().equals(clusterProperties.getLocal())) {
                configurableEmbeddedServletContainer.setPort(clusterNode.getHttp_port());
            }
        });
    }
}

