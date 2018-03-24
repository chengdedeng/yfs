/*
 * Copyright 2018-present yangguo@outlook.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    private int qos_max_time;
    private long max_upload_size;
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
