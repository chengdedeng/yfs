package info.yangguo.yfs.controller;

import info.yangguo.yfs.dto.FileMetadata;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;

@Controller
@RequestMapping(value = "api/file")
@Api(value = "api/file")
public class FileController {
    private static Logger logger = LoggerFactory.getLogger(FileController.class);

    @ApiOperation(value = "upload")
    @ResponseBody
    @RequestMapping(value = "upload", method = {RequestMethod.PUT})
    public void save(@RequestBody @Valid FileMetadata fileMetadata, @RequestParam("file") MultipartFile file) {
        /* todo */
    }
}
