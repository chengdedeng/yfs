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
package info.yangguo.yfs.common.po;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StoreInfo {
    private String group;
    private String nodeId;
    private String ip;
    private int storeHttpPort;
    private int storeSocketPort;
    private int gatewaySocketPort;
    private long updateTime;
    private long metadataFreeSpaceKb;
    private long fileFreeSpaceKb;

    private StoreInfo(){}

    public StoreInfo(String group, String nodeId, String ip, int storeHttpPort, int storeSocketPort, int gatewaySocketPort, long updateTime, Long metadataFreeSpaceKb, Long fileFreeSpaceKb) {
        this.group = group;
        this.nodeId = nodeId;
        this.ip = ip;
        this.storeHttpPort = storeHttpPort;
        this.storeSocketPort = storeSocketPort;
        this.gatewaySocketPort = gatewaySocketPort;
        this.updateTime = updateTime;
        this.metadataFreeSpaceKb = metadataFreeSpaceKb;
        this.fileFreeSpaceKb = fileFreeSpaceKb;
    }
}
