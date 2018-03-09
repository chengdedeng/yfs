package info.yangguo.yfs.config;

import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.messaging.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.filter.CharacterEncodingFilter;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import javax.servlet.Filter;
import java.util.stream.Collectors;

@Configuration
public class WebConfig extends WebMvcConfigurerAdapter {
    @Autowired
    private ClusterProperties clusterProperties;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedMethods("GET", "PUT", "POST", "DELETE", "OPTIONS");
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public Filter characterEncodingFilter() {
        CharacterEncodingFilter characterEncodingFilter = new CharacterEncodingFilter();
        characterEncodingFilter.setEncoding("UTF-8");
        characterEncodingFilter.setForceEncoding(true);
        return characterEncodingFilter;
    }

    @Bean
    public MappingJackson2HttpMessageConverter converter() {
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        return converter;
    }

    @Bean
    public StandardServletMultipartResolver getStandardServletMultipartResolver() {
        return new StandardServletMultipartResolver();
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/img/**").addResourceLocations("classpath:/static/img/");
        super.addResourceHandlers(registry);
    }

    @Bean
    public MultipartResolver multipartResolver() {
        CommonsMultipartResolver multipartResolver = new CommonsMultipartResolver();
        multipartResolver.setMaxUploadSize(5364763400000L);
        return multipartResolver;
    }

    @Bean
    public Atomix createAtomix() {
        Atomix.Builder builder = Atomix.builder();
        clusterProperties.getNode().stream().forEach(clusterNode -> {
            if (clusterNode.getId().equals(clusterProperties.getLocal())) {
                builder
                        .withLocalNode(Node.builder(clusterNode.getId())
                                .withType(Node.Type.DATA)
                                .withEndpoint(Endpoint.from(clusterNode.getHost(), clusterNode.getPort()))
                                .build());
            }
        });

        builder.withBootstrapNodes(clusterProperties.getNode().parallelStream().map(clusterNode -> {
            return Node
                    .builder(clusterNode.getId())
                    .withType(Node.Type.DATA)
                    .withEndpoint(Endpoint.from(clusterNode.getHost(), clusterNode.getPort())).build();
        }).collect(Collectors.toList()));

        Atomix atomix = builder.build();
        atomix.start().join();
        return atomix;
    }
}