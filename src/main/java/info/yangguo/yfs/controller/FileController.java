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

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.atomix.utils.time.Versioned;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;

@Controller
public class FileController {
    private static Logger logger = LoggerFactory.getLogger(FileController.class);
    @Autowired
    private ClusterProperties clusterProperties;

    @ApiOperation(value = "api/file/{qos}")
    @ResponseBody
    @RequestMapping(value = "api/file/{qos}", method = {RequestMethod.POST})
    public Result upload(@PathVariable int qos, MultipartFile file) {
        logger.info("upload file:{}", file.getOriginalFilename());
        Result result = new Result();
        FileMetadata fileMetadata = null;
        if (qos < 1 || qos > clusterProperties.getNode().size()) {
            result.setCode(ResultCode.C403.getCode());
            result.setValue(ResultCode.C403.getDesc());
        } else {
            try {
                CommonsMultipartFile commonsMultipartFile = (CommonsMultipartFile) file;
                fileMetadata = FileService.store(clusterProperties, commonsMultipartFile);
                boolean qosResult = MetadataService.create(clusterProperties, fileMetadata, qos);
                if (qosResult == true) {
                    result.setCode(ResultCode.C200.code);
                } else {
                    result.setCode(ResultCode.C202.code);
                }
                result.setValue(fileMetadata.getGroup() + "/" + fileMetadata.getPartition() + "/" + fileMetadata.getName());
            } catch (Exception e) {
                logger.error("upload api:{}", e);
                if (fileMetadata != null) {
                    FileService.delete(clusterProperties, fileMetadata);
                }
                result.setCode(ResultCode.C500.getCode());
                result.setValue(ResultCode.C500.getDesc());
            }
        }
        return result;
    }

    @ApiOperation(value = "${yfs.group}/{partition}/{name:.+}")
    @RequestMapping(value = "${yfs.group}/{partition}/{name:.+}", method = {RequestMethod.DELETE})
    @ResponseBody
    public String delete(@PathVariable String partition, @PathVariable String name) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setGroup(clusterProperties.getGroup());
        fileMetadata.setPartition(Integer.valueOf(partition));
        fileMetadata.setName(name);
        logger.info("delete file:{}", MetadataService.getKey(fileMetadata));
        Result result = new Result();
        try {
            if (MetadataService.softDelete(clusterProperties, fileMetadata)) {
                FileService.delete(clusterProperties, fileMetadata);
                result.setCode(ResultCode.C200.code);
            }
        } catch (Exception e) {
            logger.error("delete api:{}", e);
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return JsonUtil.toJson(result, true);
    }

    @ApiOperation(value = "${yfs.group}/{partition}/{name:.+}")
    @RequestMapping(value = "${yfs.group}/{partition}/{name:.+}", method = {RequestMethod.GET})
    public void download(@PathVariable String partition, @PathVariable String name, HttpServletResponse response) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setGroup(clusterProperties.getGroup());
        fileMetadata.setPartition(Integer.valueOf(partition));
        fileMetadata.setName(name);
        try {
            response.addHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(name, "UTF-8"));
            response.setContentType("application/octet-stream");
            FileService.getFile(clusterProperties, fileMetadata, response);
        } catch (Exception e) {
            logger.error("file:{}下载失败:{}", JsonUtil.toJson(fileMetadata, true), e);
        }
    }

    @ApiOperation(value = "${yfs.group}/metadata")
    @RequestMapping(value = "${yfs.group}/metadata", method = {RequestMethod.GET})
    @ResponseBody
    public Result metadata() {
        Result result = new Result<>();
        try {
            Map<String, FileMetadata> metadata = YfsConfig.consistentMap.asJavaMap();
            result.setValue(metadata);
            result.setCode(ResultCode.C200.getCode());
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return result;
    }

    @ApiOperation(value = "admin/${yfs.group}/resync/{node}")
    @RequestMapping(value = "admin/${yfs.group}/resync/{node}", method = {RequestMethod.PATCH})
    @ResponseBody
    public Result resyncNode(@PathVariable String node) {
        Result result = new Result<>();
        HashSet<String> anomalyFile = new HashSet<>();
        try {
            YfsConfig.consistentMap.values().stream().forEach(fileMetadataVersioned -> {
                long version = fileMetadataVersioned.version();
                FileMetadata fileMetadata = fileMetadataVersioned.value();
                fileMetadata.getAddNodes().remove(node);
                try {
                    if (false == YfsConfig.consistentMap.replace(MetadataService.getKey(fileMetadata), version, fileMetadata)) {
                        anomalyFile.add(MetadataService.getKey(fileMetadata));
                    }
                } catch (Exception e) {
                    anomalyFile.add(MetadataService.getKey(fileMetadata));
                }
            });
            if (anomalyFile.size() == 0) {
                result.setCode(ResultCode.C200.getCode());
            } else {
                result.setCode(ResultCode.C202.getCode());
                result.setValue(anomalyFile);
            }
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return result;
    }

    @ApiOperation(value = "admin/${yfs.group}/resync/{node}/{partition}/{name:.+}")
    @RequestMapping(value = "admin/${yfs.group}/resync/{node}/{partition}/{name:.+}", method = {RequestMethod.PATCH})
    @ResponseBody
    public String resyncFile(@PathVariable String node, @PathVariable String partition, @PathVariable String name) {
        Result result = new Result<>();
        try {
            String key = MetadataService.getKey(clusterProperties.getGroup(), partition, name);
            Versioned<FileMetadata> versioned = YfsConfig.consistentMap.get(key);
            long version = versioned.version();
            FileMetadata fileMetadata = versioned.value();
            fileMetadata.getAddNodes().remove(node);
            if (false == YfsConfig.consistentMap.replace(key, version, fileMetadata)) {
                result.setCode(ResultCode.C202.getCode());
            } else {
                result.setCode(ResultCode.C200.getCode());
            }
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return JsonUtil.toJson(result, true);
    }
}
