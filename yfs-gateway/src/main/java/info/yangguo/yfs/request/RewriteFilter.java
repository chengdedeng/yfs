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
package info.yangguo.yfs.request;

import info.yangguo.yfs.common.utils.IdMaker;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RewriteFilter implements RequestFilter {
    public HttpResponse doFilter(HttpRequest originalRequest, HttpObject httpObject) {
        if (httpObject instanceof HttpRequest) {
            if (HttpMethod.POST == originalRequest.method()) {
                ((HttpRequest) httpObject).setUri("http://upload" + ((HttpRequest) httpObject).uri());
            } else {
                String uri = ((HttpRequest) httpObject).uri();
                if (uri.endsWith("swagger-resources/configuration/ui")
                        || uri.endsWith("swagger-resources/configuration/security")
                        || uri.endsWith("swagger-resources")
                        || uri.endsWith("v2/api-docs")
                        || uri.endsWith("swagger-ui.html")
                        || uri.contains("webjars")) {
                    ((HttpRequest) httpObject).setUri("http://upload" + uri);
                } else {
                    Pattern pathPattern = Pattern.compile("/\\w{1,3}/\\w{1,3}/\\d{10,}(\\.\\w+)*+");
                    Matcher matcher = pathPattern.matcher(uri);
                    if (matcher.matches()) {
                        String[] uriParts = uri.split("/");
                        String id = uriParts[uriParts.length - 1].split("\\.")[0];
                        String[] idParts = IdMaker.INSTANCE.split(id);
                        ((HttpRequest) httpObject).setUri("http://" + idParts[0] + uri);
                    }
                }
            }
        }
        return null;
    }
}
