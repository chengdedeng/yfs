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

@Controller
public class FileController {
    private static Logger logger = LoggerFactory.getLogger(FileController.class);
    @Autowired
    private ClusterProperties clusterProperties;

    @ApiOperation(value = "api/file")
    @ResponseBody
    @RequestMapping(value = "api/file", method = {RequestMethod.POST})
    public Result upload(MultipartFile file) {
        logger.info("upload file:{}", file.getOriginalFilename());
        Result result = new Result();
        FileMetadata fileMetadata = null;
        try {
            CommonsMultipartFile commonsMultipartFile = (CommonsMultipartFile) file;
            fileMetadata = FileService.store(clusterProperties, commonsMultipartFile);
            MetadataService.create(clusterProperties, fileMetadata);
            result.setCode(ResultCode.C200.code);
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

    @ApiOperation(value = "{group}/{partition}/{name:.+}")
    @RequestMapping(value = "{group}/{partition}/{name:.+}", method = {RequestMethod.DELETE})
    @ResponseBody
    public String delete(@PathVariable String group, @PathVariable String partition, @PathVariable String name) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setGroup(group);
        fileMetadata.setPartition(Integer.valueOf(partition));
        fileMetadata.setName(name);
        logger.info("delete file:{}", MetadataService.getKey(fileMetadata));
        Result result = new Result();
        try {
            MetadataService.softDelete(clusterProperties, fileMetadata);
            FileService.delete(clusterProperties, fileMetadata);
            result.setCode(ResultCode.C200.code);
        } catch (Exception e) {
            logger.error("delete api:{}", e);
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return JsonUtil.toJson(result,true);
    }

    @ApiOperation(value = "{group}/{partition}/{name:.+}")
    @RequestMapping(value = "{group}/{partition}/{name:.+}", method = {RequestMethod.GET})
    public void download(@PathVariable String group, @PathVariable String partition, @PathVariable String name, HttpServletResponse response) {
        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setGroup(group);
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
}
