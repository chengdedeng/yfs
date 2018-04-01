package info.yangguo.yfs.controller;

import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.utils.JsonUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;

public abstract class BaseController {
    protected static Logger logger = LoggerFactory.getLogger(BaseController.class);

    protected void outputResult(HttpServletResponse response, Result result) {
        response.setContentType("application/json;charset=UTF-8");
        try (OutputStream outputStream = response.getOutputStream()) {
            IOUtils.copy(new StringReader(JsonUtil.toJson(result, true)), outputStream);
        } catch (IOException e) {
            logger.error("Output Result Error", e);
        }
    }
}
