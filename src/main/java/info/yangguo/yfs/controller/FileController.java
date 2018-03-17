package info.yangguo.yfs.controller;

import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.FileService;
import info.yangguo.yfs.service.MetadataService;
import io.atomix.core.Atomix;
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
    @Autowired
    private Atomix atomix;

    @ApiOperation(value = "api/file")
    @ResponseBody
    @RequestMapping(value = "api/file", method = {RequestMethod.POST})
    public Result upload(MultipartFile file) {
        Result result = new Result();
        try {
            CommonsMultipartFile commonsMultipartFile = (CommonsMultipartFile) file;
            FileMetadata fileMetadata = FileService.store(clusterProperties, commonsMultipartFile);
            MetadataService.create(clusterProperties, atomix, fileMetadata);
            result.setCode(ResultCode.C200.code);
            result.setValue(fileMetadata.getGroup() + "/" + fileMetadata.getPartition() + "/" + fileMetadata.getName());
        } catch (Exception e) {
            logger.error("upload api:{}", e);
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        return result;
    }

    @ApiOperation(value = "{group}/{partition}/{name}")
    @RequestMapping(value = "{group}/{partition}/{name:.+}", method = {RequestMethod.GET})
    public void download(@PathVariable String group, @PathVariable String partition, @PathVariable String name, HttpServletResponse response) {
        String filePath = MetadataService.getId(group, partition, name);
        try {
            response.addHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(name, "UTF-8"));
            response.addHeader("Content-Length", "" + MetadataService.getFileMetadata(atomix, filePath).getSize());
            response.setContentType("application/octet-stream");
            FileService.getFile(clusterProperties, filePath, response);
        } catch (Exception e) {
            logger.error("file:{}下载失败:{}", filePath, e);
        }
    }
}
