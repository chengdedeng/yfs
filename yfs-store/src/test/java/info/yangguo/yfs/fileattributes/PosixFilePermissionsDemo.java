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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * 
 * @author Frank Dietrich <Frank.Dietrich@gmx.li>
 */
public class PosixFilePermissionsDemo {

    public static void main(String[] args) {

        try {
            Set<PosixFilePermission> posixPerms;

            System.out.println("### obtain Posix file permissions ###");
            Path path = Paths.get("resources/lorem-ipsum.txt");
            posixPerms = Files.getPosixFilePermissions(path);
            System.out.println("posixPerms as enum  : " + posixPerms);
            System.out.println("posixPerms as String: " + PosixFilePermissions.toString(posixPerms));

            System.out.printf("%n### set Posix file permissions ###%n");
            path = Paths.get("tmp/");
            Path pathTmpFile = Files.createTempFile(path, "posix-perms_", ".tmp");
            pathTmpFile.toFile().deleteOnExit();
            System.out.printf("--- temporary file created%n");
            System.out.println(pathTmpFile);

            System.out.printf("%n--- default permission of temporary file%n");
            showPosixPerms(pathTmpFile);

            // change Posix permissions
            System.out.printf("%n--- change temporary file permission using PosixFilePermissions.fromString()%n");
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-r--r--");
            Files.setPosixFilePermissions(pathTmpFile, perms);
            showPosixPerms(pathTmpFile);

            System.out.printf("%n--- change temporary file permission using enum PosixFilePermission%n");
            perms.clear();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            perms.add(PosixFilePermission.GROUP_READ);
            perms.add(PosixFilePermission.GROUP_WRITE);
            perms.add(PosixFilePermission.OTHERS_READ);
            Files.setPosixFilePermissions(pathTmpFile, perms);
            showPosixPerms(pathTmpFile);
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }

    }

    private static void showPosixPerms(Path pathTmpFile) throws IOException {
        Set<PosixFilePermission> posixPerms;
        posixPerms = Files.getPosixFilePermissions(pathTmpFile);
        System.out.printf("posixPerms of temp file: %s%n", PosixFilePermissions.toString(posixPerms));
    }
}
