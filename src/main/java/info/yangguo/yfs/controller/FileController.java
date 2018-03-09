package info.yangguo.yfs.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.IOException;
import java.io.InputStream;

@Controller
@RequestMapping(value = "api/file")
@Api(value = "api/file")
public class FileController {
    private static Logger logger = LoggerFactory.getLogger(FileController.class);

    @ApiOperation(value = "upload")
    @ResponseBody
    @RequestMapping(value = "upload", method = {RequestMethod.POST})
    public void save(@RequestParam("file") CommonsMultipartFile file) {
        long size = file.getFileItem().getSize();
        int bufSize = 1024 * 1024 * 10;
        try {
            InputStream inputstream = file.getInputStream();
            for (int i = 1; i < size / bufSize + 1; i++) {
                if (bufSize * i > size) {
                    IOUtils.toByteArray(inputstream, size - (bufSize * (i - 1)));
                } else {
                    IOUtils.toByteArray(inputstream, bufSize);
                }
                System.out.println(i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
