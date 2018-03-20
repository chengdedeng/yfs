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

import com.google.common.hash.Hashing;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.po.FileMetadata;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Date;
import java.util.HashSet;

public class FileService {
    public static FileMetadata store(ClusterProperties clusterProperties, CommonsMultipartFile commonsMultipartFile) throws IOException {
        FileMetadata fileMetadata = new FileMetadata();
        int block = (Hashing.consistentHash(Hashing.murmur3_32().hashBytes(commonsMultipartFile.getOriginalFilename().getBytes()), clusterProperties.getFiledata().getPartition()) + 1);
        fileMetadata.setCreateTime(new Date());
        fileMetadata.setGroup(clusterProperties.getGroup());
        fileMetadata.setName(commonsMultipartFile.getOriginalFilename());
        fileMetadata.setSize(commonsMultipartFile.getSize());
        fileMetadata.setPartition(block);
        fileMetadata.setAddTargetNodes(new HashSet<>());
        fileMetadata.setRemoveTargetNodes(new HashSet<>());

        long checkSum = store(clusterProperties, fileMetadata, commonsMultipartFile.getInputStream());
        fileMetadata.setCheckSum(checkSum);
        return fileMetadata;
    }

    public static long store(ClusterProperties clusterProperties, FileMetadata fileMetadata, HttpResponse httpResponse) throws IOException {
        return store(clusterProperties, fileMetadata, httpResponse.getEntity().getContent());
    }


    private static long store(ClusterProperties clusterProperties, FileMetadata fileMetadata, InputStream inputStream) throws IOException {
        long checkSum;
        StringBuilder fileDir = getDir(clusterProperties, fileMetadata);
        File dir = new File(fileDir.toString());
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
        }
        File file = new File(fileDir + "/" + fileMetadata.getName());
        if (file.exists()) {
            file.delete();
            file = new File(fileDir + "/" + fileMetadata.getName());
        }
        try {
            FileUtils.copyInputStreamToFile(inputStream, file);
            checkSum = FileUtils.checksumCRC32(file);
        } catch (IOException e) {
            file.deleteOnExit();
            throw e;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return checkSum;
    }

    public static void getFile(ClusterProperties clusterProperties, FileMetadata fileMetadata, HttpServletResponse response) throws IOException {
        StringBuilder filePath = getPath(clusterProperties, fileMetadata);
        File file = new File(filePath.toString());
        try (InputStream inputStream = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()) {
            IOUtils.copyLarge(inputStream, outputStream);
        } catch (IOException e) {
            throw e;
        }
    }

    public static void delete(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        StringBuilder filePath = getPath(clusterProperties, fileMetadata);
        File file = new File(filePath.toString());
        file.delete();
    }

    private static StringBuilder getDir(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        StringBuilder fileDir = new StringBuilder();
        if (!clusterProperties.getFiledata().getDir().startsWith("/")) {
            fileDir.append(FileUtils.getUserDirectoryPath()).append("/");
        }
        fileDir.append(clusterProperties.getFiledata().getDir())
                .append("/")
                .append(clusterProperties.getLocal())
                .append("/")
                .append(clusterProperties.getGroup())
                .append("/")
                .append(fileMetadata.getPartition());
        return fileDir;
    }

    private static StringBuilder getPath(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        return getDir(clusterProperties, fileMetadata)
                .append("/")
                .append(fileMetadata.getName());
    }

    public static long checkFile(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        long checkSum = 0L;
        String filePath = getPath(clusterProperties, fileMetadata).toString();
        try {
            File file = new File(filePath);
            if (file.exists()) {
                checkSum = FileUtils.checksumCRC32(new File(filePath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkSum;
    }
}
