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
package info.yangguo.yfs.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import io.atomix.core.iterator.SyncIterator;
import io.atomix.core.map.AtomicMap;
import io.atomix.utils.time.Versioned;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping(value = "${yfs.group}/metadata")
public class MetadataController extends BaseController {
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private YfsConfig yfsConfig;

    @ApiOperation(value = "get all file metadata by paging query")
    @RequestMapping(value = "file", method = {RequestMethod.GET})
    public void getAllFileMetadata(@ApiParam(value = "每页的条数", required = true) @RequestParam("pageSize") Integer pageSize, @ApiParam(value = "页码，起始码为1。", required = true) @RequestParam(value = "page") Integer page, HttpServletResponse response) {
        Result result = new Result<>();
        Map<String, Object> values = Maps.newHashMap();
        try {
            AtomicMap<String, FileMetadata> atomicMap = yfsConfig.fileMetadataMap;
            values.put("count", atomicMap.size());
            values.put("page", page);
            values.put("pageSize", pageSize);
            Integer begin = (page - 1) * pageSize;
            Integer end = page * pageSize;
            List<FileMetadata> metadataList = Lists.newArrayList();
            if (begin < atomicMap.size()) {
                SyncIterator<Map.Entry<String, Versioned<FileMetadata>>> iterator = atomicMap.entrySet().iterator();
                Integer i = 0;
                while (iterator.hasNext()) {
                    if (i >= begin && i < end) {
                        Map.Entry<String, Versioned<FileMetadata>> entry = iterator.next();
                        metadataList.add(entry.getValue().value());
                    } else if (i > end) {
                        break;
                    }
                    i++;
                }
            }
            values.put("values", metadataList);

            result.setValue(values);
            result.setCode(ResultCode.C200.getCode());
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        outputResult(response, result);
    }

//    @ApiOperation(value = "get metadata for one file")
//    @RequestMapping(value = "file/{partition}//{name:.+}", method = {RequestMethod.GET})
//    public void getOneFileMetadata(@PathVariable Integer partition, @PathVariable String name, HttpServletResponse response) {
//        Result result = new Result<>();
//        try {
//            FileMetadata metadata = YfsConfig.fileMetadataMap.get(key).value();
//            result.setValue(metadata);
//            result.setCode(ResultCode.C200.getCode());
//        } catch (Exception e) {
//            result.setCode(ResultCode.C500.getCode());
//            result.setValue(ResultCode.C500.getDesc());
//        }
//        outputResult(response, result);
//    }
}
