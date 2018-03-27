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
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import info.yangguo.yfs.utils.JsonUtil;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;

@Controller
public class FileController extends BaseController {
    @Autowired
    private ClusterProperties clusterProperties;

    @ApiOperation(value = "api/file")
    @ResponseBody
    @RequestMapping(value = "api/file", method = {RequestMethod.POST})
    public Result upload(@RequestParam(value = "qos", required = false) Integer qos, MultipartFile file) {
        logger.info("upload file:{}", file.getOriginalFilename());
        Result result = new Result();
        FileMetadata fileMetadata = null;
        if (qos < 1) {
            qos = 1;
        } else if (qos > clusterProperties.getNode().size()) {
            qos = clusterProperties.getNode().size();
        }
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
        return result;
    }

    @ApiOperation(value = "${yfs.group}/{partition}/{name:.+}")
    @RequestMapping(value = "${yfs.group}/{partition}/{name:.+}", method = {RequestMethod.DELETE})
    public void delete(@PathVariable String partition, @PathVariable String name, HttpServletResponse response) {
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
        outputResult(response, result);
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
            logger.error("download file:{}", JsonUtil.toJson(fileMetadata, true), e);
        }
    }
}
