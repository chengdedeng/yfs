/*
 * Copyright 2015 Frank Dietrich <Frank.Dietrich@gmx.li>.
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.UserDefinedFileAttributeView;

/**
 *
 * @author Frank Dietrich <Frank.Dietrich@gmx.li>
 */
public class UserExtendedAttributes {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            showUsage();
            return;
        }

        if ("--info".equals(args[0])) {
            infoFileStores();
        } else if ("--set".equalsIgnoreCase(args[0]) && args.length == 3) {
            setXattr(args[1], args[2]);
        } else if ("--get".equalsIgnoreCase(args[0]) && args.length == 3) {
            getXattr(args[1], args[2]);
        } else if ("--del".equalsIgnoreCase(args[0]) && args.length == 3) {
            delXattr(args[1], args[2]);
        } else if ("--dump".equalsIgnoreCase(args[0]) && args.length == 2) {
            dumpAllXattr(args[1]);
        } else {
            showUsage();
        }
    }

    private static void showUsage() {
        System.out.println("usage: UserXattr --set user_xattr=value filename");
        System.out.println("        UserXattr --get user_xattr filename");
        System.out.println("        UserXattr --dump filename");
        System.out.println("        UserXattr --del user_xattr filename");
        System.out.println("        UserXattr --info");
        System.out.println("");
        System.out.println("  --set   set the user defined attribute 'user_xattr' of the file to 'value'");
        System.out.println("  --get   prints the value of the user defined attribute 'user_xattr' of the file");
        System.out.println("  --dump  prints all user defined attributes 'user_xattr' and their values");
        System.out.println("  --del   removes the user defined attribute 'user_xattr' of the file");
        System.out.println("  --info  show which partition/filesystem reported supporting extended file attributes");
    }

    /**
     * Show all {@link FileStore} and their capability to support user extended
     * attributes.<br>
     * For filesystems ext3 and ext4 the reported capability might be reported
     * as {@code not supported} but might work anyway.
     */
    private static void infoFileStores() {
        FileSystem fs = FileSystems.getDefault();
        int maxNameLength = 0;
        for (FileStore fileStore : fs.getFileStores()) {
            int nameLength = fileStore.toString().length();
            maxNameLength = maxNameLength < nameLength ? nameLength : maxNameLength;
        }
        String format = "fileStore: %-" + maxNameLength + "s  type: %s";
        for (FileStore fileStore : fs.getFileStores()) {
            if (fileStore.toString().startsWith("/sys")) {
                continue;
            }
            System.out.printf(format, fileStore, fileStore.type());
            if (hasUserXattrSupport(fileStore)) {
                System.out.print(" xattr supported");
            } else if ("ext3".equals(fileStore.type())
                    || "ext4".equals(fileStore.type())) {
                // the reported capability might be wrong, execute following
                // command to correct it
                // echo "ext4=user_xattr" >> $JAVA_HOME/lib/fstypes.properties
                System.out.print(" no xattr support (but anyway might work)");
            } else {
                System.out.print(" no xattr support");
            }
            System.out.println("");
        }
    }

    /**
     * @return if the passed {@link FileStore} has support for user extended
     * attributes
     */
    private static boolean hasUserXattrSupport(FileStore fileStore) {
        return fileStore.supportsFileAttributeView(UserDefinedFileAttributeView.class);
    }

    /**
     * Set an user extended attribute for the given file.
     *
     * @param attr the attribut consist of the name and the value in the format
     * {@code name=value}
     * @param file the filename for which the attribut should be set
     */
    private static void setXattr(String attr, String file) {
        UserDefinedFileAttributeView view = getAttributeView(file);

        String[] xattr = attr.split("=");
        if (xattr.length < 2) {
            System.out.println("the extended attribute and value must follow the format: name=value");
            return;
        }
        String name = xattr[0];
        String value = xattr[1];

        try {
            view.write(name, Charset.defaultCharset().encode(value));
        } catch (Exception ex) {
            System.out.printf("could not set attr '%s=%s' for %s%n%s%n", name, value, file, ex);
        }
    }

    /**
     * Show the value of a user extended attribut stored on the given file.
     *
     * @param name the name of the user extended attribute
     * @param file the filename from which the attribute should be read
     * @throws IOException when the user extended attribute information could
     * not be read
     */
    private static void getXattr(String name, String file) {
        UserDefinedFileAttributeView view = getAttributeView(file);

        try {
            Charset defaultCharset = Charset.defaultCharset();
            if (view.list().contains(name)) {
                System.out.printf("# file: %s%n", file);
                String value = getAttrValue(view, name);
                System.out.printf("user.%s=\"%s\"%n", name, value);
            } else {
                System.out.printf("file has no extended attribute [%s]%n", name);
            }
        } catch (IOException ex) {
            System.out.printf("failed to get the extended attribute %s from %s%n", name, file);
        }
    }

    /**
     * List all user extended attributes and their related values for the given
     * file.
     *
     * @throws IOException when the user extended attribute information could
     * not be read
     */
    private static void dumpAllXattr(String file) {
        UserDefinedFileAttributeView view = getAttributeView(file);

        System.out.printf("# file: %s%n", file);
        try {
            for (String name : view.list()) {
                String value = getAttrValue(view, name);
                System.out.printf("user.%s=\"%s\"%n", name, value);
            }
        } catch (IOException ex) {
            System.out.printf("failed to get the extended attribute informations from %s%n", file);
        }
    }

    /**
     * Delete a user extended attribute from the given file.
     *
     * @param name the name of the attribute which should be deleted
     * @param file the filename from which the attribute should be deleted
     * @throws IOException when the user extended attribute information could
     * not be read
     */
    private static void delXattr(String name, String file) {
        UserDefinedFileAttributeView view = getAttributeView(file);
        try {
            view.delete(name);
        } catch (IOException ex) {
            System.out.println("failed to delete the user extended attribute: " + ex.getMessage());
        }
    }

    /**
     * @param view the attribute view of the extendet attribute for a specific
     * file
     * @param name the name of the attribute for which the value should be
     * returned
     * @return the value of an user extended attribute from the given attribute
     * view
     */
    private static String getAttrValue(UserDefinedFileAttributeView view, String name) throws IOException {
        int attrSize = view.size(name);
        ByteBuffer buffer = ByteBuffer.allocateDirect(attrSize);
        view.read(name, buffer);
        buffer.flip();
        return Charset.defaultCharset().decode(buffer).toString();
    }

    /**
     * Returns the attribute view of the extended attribute for the given file.
     *
     * @param file the filename for which the attribute view should be returned
     * @return
     */
    private static UserDefinedFileAttributeView getAttributeView(String file) {
        return Files.getFileAttributeView(Paths.get(file),
                UserDefinedFileAttributeView.class);
    }
}
