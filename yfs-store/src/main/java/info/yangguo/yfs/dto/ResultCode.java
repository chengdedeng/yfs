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
package info.yangguo.yfs.dto;

public enum ResultCode {
    C200(200, "Success"),
    C202(202, "Accepted"),
    C403(403, "Forbidden"),
    C500(500, "Internal Server Error");

    ResultCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int code;//code
    public String desc;//description

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
