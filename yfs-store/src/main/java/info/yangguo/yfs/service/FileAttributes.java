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
package info.yangguo.yfs.service;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Map;

public class FileAttributes {

    public static BasicFileAttributes getBasicAttrs(String fullPath) throws IOException {
        Path path = Paths.get(fullPath);
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        return attributes;
    }

    public static void setXattr(Map<String, String> attrs, String fullPath) throws IOException {
        UserDefinedFileAttributeView view = getAttributeView(fullPath);

        for (Map.Entry<String, String> entry : attrs.entrySet()) {
            view.write(entry.getKey(), Charset.defaultCharset().encode(entry.getValue()));
        }
    }

    public static String getXattr(String name, String fullPath) throws IOException {
        String value = null;
        UserDefinedFileAttributeView view = getAttributeView(fullPath);
        if (view.list().contains(name)) {
            value = getAttrValue(view, name);
        }
        return value;
    }


    public static Map<String, String> getAllXattr(String fullPath) throws IOException {
        Map<String, String> values = Maps.newHashMap();
        UserDefinedFileAttributeView view = getAttributeView(fullPath);
        for (String name : view.list()) {
            String value = getAttrValue(view, name);
            values.put(name, value);
        }
        return values;
    }

    public static void delXattr(String name, String file) throws IOException {
        UserDefinedFileAttributeView view = getAttributeView(file);
        view.delete(name);
    }

    private static String getAttrValue(UserDefinedFileAttributeView view, String name) throws IOException {
        int attrSize = view.size(name);
        ByteBuffer buffer = ByteBuffer.allocateDirect(attrSize);
        view.read(name, buffer);
        buffer.flip();
        return Charset.defaultCharset().decode(buffer).toString();
    }

    private static UserDefinedFileAttributeView getAttributeView(String fullPath) {
        return Files.getFileAttributeView(Paths.get(fullPath),
                UserDefinedFileAttributeView.class);
    }
}
