package info.yangguo.yfs.config;

import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ExceptionHandler implements HandlerExceptionResolver {
    private static Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);


    public ModelAndView resolveException(HttpServletRequest request, HttpServletResponse response, Object handler,
                                         Exception ex) {
        Map<String, Object> result = new HashMap();
        if (ex instanceof MaxUploadSizeExceededException) {
            result.put("code", ResultCode.C403.code);
            result.put("msg", "Maximum upload size of " + ((MaxUploadSizeExceededException) ex).getMaxUploadSize() + " bytes exceeded");
            logger.warn(ex.getMessage());
        } else {
            result.put("code", ResultCode.C500.code);
            result.put("msg", ResultCode.C500.desc);
            logger.error("Internal server error", ex);
        }
        printWrite(JsonUtil.toJson(result, true), response);
        return new ModelAndView();
    }

    @Bean
    public ExceptionHandler createExceptionHandler() {
        return new ExceptionHandler();
    }
    public static void printWrite(String msg, HttpServletResponse response) {
        try {
            PrintWriter pw = response.getWriter();
            pw.write(msg);
            pw.flush();
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
