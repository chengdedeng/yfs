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

import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.dto.Result;
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
