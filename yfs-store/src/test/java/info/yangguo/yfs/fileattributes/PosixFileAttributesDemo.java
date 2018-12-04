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
import java.nio.file.attribute.PosixFileAttributes;
import java.util.Map;

/**
 * 
 * @author Frank Dietrich <Frank.Dietrich@gmx.li>
 */
public class PosixFileAttributesDemo {

    public static void main(String[] args) {
        // regular file
        Path path = Paths.get("resources/lorem-ipsum.txt");

        // as example for file type
        // create a named pipe: 'mknod /tmp/named.pipe p'
        // Path path = Paths.get("/tmp/named.pipe");
        try {
            System.out.printf("### single access to file attributes%n");
            PosixFileAttributes attr;
            attr = Files.readAttributes(path, PosixFileAttributes.class, NOFOLLOW_LINKS);
            String outFormat = "%-20s: %s%n";
            System.out.printf(outFormat, "creationTime", attr.creationTime());
            System.out.printf(outFormat, "lastAccessTime", attr.lastAccessTime());
            System.out.printf(outFormat, "lastModifiedTime", attr.lastModifiedTime());
            System.out.printf(outFormat, "isRegularFile", attr.isRegularFile());
            System.out.printf(outFormat, "isDirectory", attr.isDirectory());
            System.out.printf(outFormat, "isSymbolicLink", attr.isSymbolicLink());
            // could be a named pipe for example
            System.out.printf(outFormat, "isOther", attr.isOther());
            System.out.printf(outFormat, "size", attr.size());
            // object which uniquely identifies the given file
            // on UNIX: it contains the device id and the inode
            System.out.printf(outFormat, "fileKey", attr.fileKey());
            System.out.printf(outFormat, "owner", attr.owner());
            System.out.printf(outFormat, "group", attr.group());
            System.out.printf(outFormat, "permissons", attr.permissions());

            System.out.printf("%n### bulk access to file attributes%n");
            Map<String, Object> attrBulk;
            attrBulk = Files.readAttributes(path, "posix:*", NOFOLLOW_LINKS);
            for (String key : attrBulk.keySet()) {
                System.out.printf("%s:%-16s: %s%n", "posix", key, attrBulk.get(key));
            }
        } catch (IOException ex) {
            System.err.println("failed to obtain PosixFileAttributes " + ex.getMessage());
        }

    }
}
