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

import info.yangguo.yfs.common.po.FileEvent;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.service.EventService;
import info.yangguo.yfs.service.FileService;
import io.atomix.utils.time.Versioned;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;

@Controller
public class FileController extends BaseController {
    @Autowired
    private ClusterProperties clusterProperties;
    @Autowired
    private YfsConfig yfsConfig;

    //不存在孤儿数据，即上传Abort时。
    //https://stackoverflow.com/questions/21089106/converting-multipartfile-to-java-io-file-without-copying-to-local-machine
    @ApiOperation(value = "upload file")
    @ResponseBody
    @RequestMapping(value = "api/file", method = {RequestMethod.POST})
    public Result upload(MultipartFile file, HttpServletRequest httpServletRequest) {
        Result result = new Result();
        Pair<String, FileEvent> pair = null;
        try {
            CommonsMultipartFile commonsMultipartFile = (CommonsMultipartFile) file;
            pair = FileService.store(clusterProperties, commonsMultipartFile, httpServletRequest);
            boolean qosResult = EventService.create(clusterProperties, yfsConfig, pair, clusterProperties.getStore().getNode().size() / 2 + 1);
            if (qosResult == true) {
                result.setCode(ResultCode.C200.code);
            } else {
                result.setCode(ResultCode.C202.code);
            }
            result.setValue(pair.getKey());
            logger.debug("Success to upload {}", pair.getKey());
        } catch (Exception e) {
            logger.error("Upload", e);
            if (pair != null)
                FileService.delete(clusterProperties, pair.getKey());
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return result;
    }

    @RequestMapping(value = "{first:\\w{1,3}}/{second:\\w{1,3}}/{name:.+}", method = {RequestMethod.DELETE})
    public void delete(@PathVariable String first, @PathVariable String second, @PathVariable String name, HttpServletResponse response) {
        String path = first + "/" + second + "/" + name;
        logger.info("delete file:{}", path);
        Result result = new Result();
        try {
            Versioned<FileEvent> fileEventVersioned = yfsConfig.fileEventMap.get(path);
            if (fileEventVersioned != null) {
                if (EventService.softDelete(clusterProperties, yfsConfig, path)) {
                    FileService.delete(clusterProperties, path);
                    result.setCode(ResultCode.C200.code);
                }
            }
        } catch (Exception e) {
            logger.error("delete api:{}", e);
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        outputResult(response, result);
    }

    @RequestMapping(value = "{first:\\w{1,3}}/{second:\\w{1,3}}/{name:.+}", method = {RequestMethod.GET})
    public void download(@PathVariable String first, @PathVariable String second, @PathVariable String name, @RequestHeader(required = false) String range, HttpServletRequest request, HttpServletResponse response) {
        String path = first + File.separator + second + File.separator + name;
        try {
            FileService.getFile(clusterProperties, path, request, response);
        } catch (Exception e) {
            logger.error("download file:{}", path, e);
        }
    }
}
