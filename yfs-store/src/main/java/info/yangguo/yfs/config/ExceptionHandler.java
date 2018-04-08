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
package info.yangguo.yfs.config;

import info.yangguo.yfs.dto.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ExceptionHandler implements HandlerExceptionResolver {
    private static Logger logger = LoggerFactory.getLogger(ExceptionHandler.class);


    public ModelAndView resolveException(HttpServletRequest request, HttpServletResponse response, Object handler,
                                         Exception ex) {
        ModelAndView mv = new ModelAndView();
        MappingJackson2JsonView view = new MappingJackson2JsonView();
        Map<String, Object> attributes = new HashMap();
        if (ex instanceof MaxUploadSizeExceededException) {
            attributes.put("code", ResultCode.C403.code);
            attributes.put("msg", "Maximum upload size of " + ((MaxUploadSizeExceededException) ex).getMaxUploadSize() + " bytes exceeded");
            logger.warn(ex.getMessage());
        } else {
            attributes.put("code", ResultCode.C500.code);
            attributes.put("msg", ResultCode.C500.desc);
            logger.error("Internal server error", ex);
        }
        view.setAttributesMap(attributes);
        mv.setView(view);
        return mv;
    }

    @Bean
    public ExceptionHandler createExceptionHandler() {
        return new ExceptionHandler();
    }
}
