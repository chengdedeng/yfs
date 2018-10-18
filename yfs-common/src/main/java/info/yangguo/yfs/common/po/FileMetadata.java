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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class FileMetadata implements Serializable {
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
    private long size;
    /**
     * store path
     */
    private String path;
    /**
     * create time
     */
    private Date time;
    /**
     * hash by md5
     */
    private String md5;
    /**
     * checksum by crc32
     */
    private Long crc32;
    /**
     * extension metadata by user
     */
    private Map<String, String> metadata;
    private List<String> addNodes = new ArrayList<>();
    private List<String> removeNodes = new ArrayList<>();
}
