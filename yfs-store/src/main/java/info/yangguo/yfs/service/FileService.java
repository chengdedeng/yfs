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
import info.yangguo.yfs.common.CommonConstant;
import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.config.ClusterProperties;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.fontbox.ttf.BufferedRandomAccessFile;
import org.apache.http.HttpResponse;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class FileService {
    private static Logger logger = LoggerFactory.getLogger(FileService.class);
    private static Map<String, Long> runningFile = new ConcurrentHashMap<>();
    /**
     * 获取文件扩展名
     */
    private static Function<String, String> getExFileName = fileName -> {
        String[] parts = fileName.split("\\.");
        if (parts.length > 1)
            return parts[parts.length - 1];
        else
            return null;
    };

    /**
     * 用户上传时的存储方法
     *
     * @param clusterProperties
     * @param commonsMultipartFile
     * @param httpServletRequest
     * @return
     * @throws IOException
     */
    public static FileMetadata store(ClusterProperties clusterProperties, CommonsMultipartFile commonsMultipartFile, HttpServletRequest httpServletRequest) throws IOException {
        Integer block1 = new Random().nextInt(clusterProperties.getStore().getFiledata().getPartition()) + 1;
        Integer block2 = new Random().nextInt(clusterProperties.getStore().getFiledata().getPartition()) + 1;
        String newName = UUID.randomUUID().toString();
        String fileName = commonsMultipartFile.getOriginalFilename();
        String exFileName = getExFileName.apply(fileName);
        String filePath = null;
        if (exFileName != null)
            filePath = clusterProperties.getGroup() + File.separator + Integer.toHexString(block1) + File.separator + Integer.toHexString(block2) + File.separator + newName + "." + exFileName;
        else
            filePath = clusterProperties.getGroup() + File.separator + Integer.toHexString(block1) + File.separator + Integer.toHexString(block2) + File.separator + newName;

        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setTime(new Date());
        fileMetadata.setPath(filePath);
        fileMetadata.setName(fileName);
        fileMetadata.setSize(commonsMultipartFile.getSize());

        Map<String, String> userMetadata = Maps.newHashMap();
        Enumeration headerNames = httpServletRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = (String) headerNames.nextElement();
            if (headerName.startsWith(CommonConstant.xHeaderPrefix)) {
                String headerValue = httpServletRequest.getHeader(headerName);
                userMetadata.put(headerName, headerValue);
            }
        }
        fileMetadata.setMetadata(userMetadata);

        try {
            store(clusterProperties, fileMetadata, commonsMultipartFile.getInputStream());
            String serverMd5 = verifyFileByMd5(clusterProperties, fileMetadata);
            String clientMd5 = httpServletRequest.getHeader(HttpHeaderNames.CONTENT_MD5.toString());
            if (clientMd5 != null) {
                if (!clientMd5.equals(serverMd5)) {
                    throw new IOException("md5 does't match");
                }
            }
            fileMetadata.setMd5(serverMd5);

            if (commonsMultipartFile.getContentType() != null) {
                fileMetadata.setType(commonsMultipartFile.getContentType());
            } else {
                try {
                    fileMetadata.setType(getContentType(clusterProperties, fileMetadata).get(Metadata.CONTENT_TYPE));
                } catch (Exception e) {
                    fileMetadata.setType(MimeTypes.OCTET_STREAM);
                }
            }
        } catch (Exception e) {
            delete(clusterProperties, fileMetadata);
        }
        return fileMetadata;
    }

    /**
     * 同步文件时的存储方法
     *
     * @param clusterProperties
     * @param fileMetadata
     * @param httpResponse
     * @throws IOException
     */
    public static void store(ClusterProperties clusterProperties, FileMetadata fileMetadata, HttpResponse httpResponse) throws IOException {
        store(clusterProperties, fileMetadata, httpResponse.getEntity().getContent());
    }

    /**
     * 底层的存储方法
     *
     * @param clusterProperties
     * @param fileMetadata
     * @param inputStream
     * @throws IOException
     */
    private static void store(ClusterProperties clusterProperties, FileMetadata fileMetadata, InputStream inputStream) throws IOException {
        String filePath = getPath(clusterProperties, fileMetadata);
        if (runningFile.putIfAbsent(filePath, new Date().getTime()) == null) {
            try {
                File file = new File(filePath);
                if (file.exists()) {
                    file.delete();
                    file = new File(filePath);
                }
                try {
                    FileUtils.copyInputStreamToFile(inputStream, file);
                } catch (IOException e) {
                    file.delete();
                    throw e;
                } finally {
                    IOUtils.closeQuietly(inputStream);
                }
                try {
                    long crc32 = verifyFileByCrc32(clusterProperties, fileMetadata);
                    if (fileMetadata.getCrc32() != null) {
                        if (fileMetadata.getCrc32().longValue() != crc32) {
                            throw new IOException("crc32 does't match");
                        }
                    } else {
                        fileMetadata.setCrc32(crc32);
                    }
                } catch (IOException e) {
                    file.delete();
                    throw e;
                }
            } catch (Exception e) {
                throw e;
            } finally {
                Long begin = runningFile.remove(filePath);
                logger.info("File[{}] Time Consuming:{}", filePath, new Date().getTime() - begin);
            }
        } else {
            throw new IOException(filePath + " In Sync");
        }
    }

    /**
     * 读取文件到HttpServletResponse的输出流
     *
     * @param clusterProperties
     * @param fileMetadata
     * @param response
     * @throws IOException
     */
    public static void getFile(ClusterProperties clusterProperties, FileMetadata fileMetadata, HttpServletRequest request, HttpServletResponse
            response) throws IOException {
        int buffSize = 256;
        //支持范围请求
        response.setHeader(Metadata.CONTENT_TYPE, fileMetadata.getType());
        response.setHeader(HttpHeaderNames.ACCEPT_RANGES.toString(), "bytes");
        response.setHeader(Metadata.CONTENT_MD5, fileMetadata.getMd5());

        String[] ranges = null;
        //http协议支持单一范围和多重范围查询
        String rangeHeader = request.getHeader(HttpHeaderNames.RANGE.toString());
        if (StringUtils.isNotBlank(rangeHeader) && rangeHeader.contains("bytes=") && rangeHeader.contains("-")) {
            rangeHeader = rangeHeader.trim();
            rangeHeader = rangeHeader.replace("bytes=", "");
            ranges = rangeHeader.split(",");
        }
        String filePath = getPath(clusterProperties, fileMetadata);
        if (ranges != null) {
            response.setStatus(HttpStatus.PARTIAL_CONTENT.value());
            if (ranges.length == 1) {//单一范围
                String range = null;
                Long start = null;
                Long end = null;
                if (ranges[0].startsWith("-")) {
                    range = "0" + ranges[0];
                } else if (ranges[0].endsWith("-")) {
                    range = ranges[0] + (fileMetadata.getSize() - 1);
                }
                String[] rangeInfo = range.split("-");
                start = Long.valueOf(rangeInfo[0]);
                end = Long.valueOf(rangeInfo[1]);

                if (start < 0 || end < 0 || start.longValue() == end.longValue()) {
                    throw new RuntimeException("range is mistake");
                } else {
                    Long contentLength = end - start + 1;
                    response.setHeader(HttpHeaderNames.CONTENT_RANGE.toString(), "bytes " + start + "-" + end + "/" + fileMetadata.getSize());
                    response.setHeader(Metadata.CONTENT_LENGTH, String.valueOf(contentLength));

                    //已传送数据大小
                    long transmitted = 0;
                    try (RandomAccessFile randomAccessFile = new BufferedRandomAccessFile(filePath, "r", buffSize); BufferedOutputStream outputStream = new BufferedOutputStream(response.getOutputStream())) {
                        byte[] buff = new byte[buffSize];
                        int length = 0;
                        randomAccessFile.seek(start);
                        //特别注意：下面判断条件的顺序不能颠倒
                        while ((transmitted + length) <= contentLength && (length = randomAccessFile.read(buff)) != -1) {
                            outputStream.write(buff, 0, length);
                            transmitted += length;
                        }
                        //处理不足buff.length部分
                        if (transmitted < contentLength) {
                            length = randomAccessFile.read(buff, 0, (int) (contentLength - transmitted));
                            outputStream.write(buff, 0, length);
                            transmitted += length;
                        }

                        outputStream.flush();
                        response.flushBuffer();
                    } catch (ClientAbortException e) {
                        logger.warn("Download {} is failing beacause client abort!", JsonUtil.toJson(fileMetadata, false));
                    } catch (IOException e) {
                        throw e;
                    }
                }
            } else {//多重范围暂时不支持
            }
        } else {//client没有进行范围查询
            response.setStatus(HttpStatus.OK.value());
            response.setHeader(Metadata.CONTENT_LENGTH, String.valueOf(fileMetadata.getSize()));

            File file = new File(filePath);
            try (InputStream inputStream = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()) {
                IOUtils.copyLarge(inputStream, outputStream, new byte[buffSize]);
            } catch (ClientAbortException e) {
                logger.warn("Download {} is failing beacause client abort!", JsonUtil.toJson(fileMetadata, false));
            } catch (IOException e) {
                throw e;
            }
        }
    }

    /**
     * 删除文件
     *
     * @param clusterProperties
     * @param fileMetadata
     */
    public static void delete(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        String filePath = getPath(clusterProperties, fileMetadata);
        File file = new File(filePath);
        file.delete();
    }

    /**
     * 获取文件在服务器端的绝对路径
     *
     * @param clusterProperties
     * @param fileMetadata
     * @return
     */
    public static String getPath(ClusterProperties clusterProperties, FileMetadata fileMetadata) {
        StringBuilder filePath = new StringBuilder();
        if (!clusterProperties.getStore().getFiledata().getDir().startsWith(File.separator)) {
            filePath.append(FileUtils.getUserDirectoryPath()).append(File.separator);
        } else {
            filePath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        }
        filePath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        filePath.append(fileMetadata.getPath());
        return filePath.toString();
    }

    /**
     * 获取文件的MD5值
     *
     * @param clusterProperties
     * @param fileMetadata
     * @return
     * @throws IOException
     */
    public static String verifyFileByMd5(ClusterProperties clusterProperties, FileMetadata fileMetadata) throws IOException {
        String filePath = getPath(clusterProperties, fileMetadata);
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath))) {
            String md5 = DigestUtils.md5Hex(fileInputStream);
            return md5;
        }
    }

    /**
     * 获取文件的CRC32值
     *
     * @param clusterProperties
     * @param fileMetadata
     * @return
     * @throws IOException
     */
    public static long verifyFileByCrc32(ClusterProperties clusterProperties, FileMetadata fileMetadata) throws IOException {
        String filePath = getPath(clusterProperties, fileMetadata);
        return FileUtils.checksumCRC32(new File(filePath));
    }

    /**
     * 通过文件获取文件类型
     *
     * @param clusterProperties
     * @param fileMetadata
     * @return
     */
    private static Metadata getContentType(ClusterProperties clusterProperties, FileMetadata fileMetadata) throws
            Exception {
        String filePath = FileService.getPath(clusterProperties, fileMetadata);
        Metadata metadata = new Metadata();
        try (FileInputStream inputStream = FileUtils.openInputStream(new File(filePath))) {
            Parser parser = new AutoDetectParser();
            parser.parse(inputStream, new BodyContentHandler(), metadata, new ParseContext());
        }
        return metadata;
    }
}
