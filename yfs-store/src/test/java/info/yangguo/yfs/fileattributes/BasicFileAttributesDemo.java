/*
 * Copyright 2014 Frank Dietrich <Frank.Dietrich@gmx.li>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package info.yangguo.yfs.fileattributes;

import java.io.IOException;
import java.nio.file.Files;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

/**
 *
 * @author Frank Dietrich <Frank.Dietrich@gmx.li>
 */
public class BasicFileAttributesDemo {

    public static void main(String[] args) {
        Path path = Paths.get("resources/lorem-ipsum.txt");
        BasicFileAttributes attr;
        try {
            attr = Files.readAttributes(path, BasicFileAttributes.class);
            String outFormat = "%-20s: %s%n";
            System.out.printf(outFormat, "creationTime", attr.creationTime());
            System.out.printf(outFormat, "lastAccessTime", attr.lastAccessTime());
            System.out.printf(outFormat, "lastModifiedTime", attr.lastModifiedTime());

            System.out.printf(outFormat, "isRegularFile", attr.isRegularFile());
            System.out.printf(outFormat, "isDirectory", attr.isDirectory());
            System.out.printf(outFormat, "isSymbolicLink", attr.isSymbolicLink());
            System.out.printf(outFormat, "size", attr.size());

            System.out.printf("%n### bulk access to file attributes%n");
            Map<String, Object> attrBulk;
            attrBulk = Files.readAttributes(path, "basic:*", NOFOLLOW_LINKS);
            for (String key : attrBulk.keySet()) {
                System.out.printf("%s:%-16s: %s%n", "basic", key, attrBulk.get(key));
            }
        } catch (IOException ex) {
            System.err.println("failed to obtain BasicFileAttributes " + ex.getMessage());
        }

    }
}
