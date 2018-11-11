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
import info.yangguo.yfs.common.po.FileEvent;
import info.yangguo.yfs.common.po.FileMetadata;
import info.yangguo.yfs.common.utils.IdMaker;
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
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class FileService {
    private static Logger logger = LoggerFactory.getLogger(FileService.class);
    private static Map<String, Long> runningFile = new ConcurrentHashMap<>();
    private static String timePattern = "EEE, dd MMM yyyy HH:mm:ss";
    private static HttpClient httpClient;

    static {
        Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(registry);
        connectionManager.setMaxTotal(1000);
        connectionManager.setDefaultMaxPerRoute(1000);

        RequestConfig requestConfig = RequestConfig.custom()
                .build();

        httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(connectionManager)
                .build();
    }

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
     * 用户上传时文件的存储方法
     *
     * @param clusterProperties
     * @param commonsMultipartFile
     * @param httpServletRequest
     * @return
     * @throws IOException
     */
    public static FileEvent store(ClusterProperties clusterProperties, CommonsMultipartFile commonsMultipartFile, HttpServletRequest httpServletRequest) throws IOException {
        FileEvent fileEvent = new FileEvent();
        Integer block1 = new Random().nextInt(clusterProperties.getStore().getFiledata().getPartition()) + 1;
        Integer block2 = new Random().nextInt(clusterProperties.getStore().getFiledata().getPartition()) + 1;
        String newName = IdMaker.INSTANCE.next(clusterProperties.getGroup(), clusterProperties.getLocal());
        String fileName = commonsMultipartFile.getOriginalFilename();
        String exFileName = getExFileName.apply(fileName);
        String relativePath = null;
        if (exFileName != null)
            relativePath = Integer.toHexString(block1) + File.separator + Integer.toHexString(block2) + File.separator + newName + "." + exFileName;
        else
            relativePath = Integer.toHexString(block1) + File.separator + Integer.toHexString(block2) + File.separator + newName;
        fileEvent.setPath(relativePath);

        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setPath(relativePath);
        fileMetadata.setName(fileName);
        fileMetadata.setSize(String.valueOf(commonsMultipartFile.getSize()));

        Map<String, String> userMetadata = Maps.newHashMap();
        Enumeration headerNames = httpServletRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = (String) headerNames.nextElement();
            if (headerName.startsWith(CommonConstant.xHeaderPrefix)) {
                String headerValue = httpServletRequest.getHeader(headerName);
                userMetadata.put(headerName, headerValue);
            }
        }
        fileMetadata.setExtension(userMetadata);

        try {
            fileEvent.setFileCrc32(store(clusterProperties, relativePath, commonsMultipartFile.getInputStream()));
            String serverMd5 = verifyFileByMd5(clusterProperties, fileMetadata);
            String clientMd5 = httpServletRequest.getHeader(HttpHeaderNames.CONTENT_MD5.toString());
            if (clientMd5 != null) {
                if (!clientMd5.equals(serverMd5)) {
                    throw new IOException("md5 does't match");
                }
            }
            fileMetadata.setMd5(serverMd5);

            try {
                fileMetadata.setType(getContentType(clusterProperties, fileMetadata).get(Metadata.CONTENT_TYPE));
            } catch (Exception e) {
                if (commonsMultipartFile.getContentType() != null) {
                    fileMetadata.setType(commonsMultipartFile.getContentType());
                } else {
                    fileMetadata.setType(MimeTypes.OCTET_STREAM);
                }
            }
            fileEvent.setMetaCrc32(storeMetadata(clusterProperties, fileMetadata));
        } catch (IOException e) {
            delete(clusterProperties, fileMetadata.getPath());
            throw e;
        }
        return fileEvent;
    }


    /**
     * 同步时文件存储方法
     *
     * @param clusterProperties
     * @param relativePath
     * @param fileUrl
     * @param crc32
     */
    public static void store(ClusterProperties clusterProperties, String relativePath, String fileUrl, String crc32) throws IOException {
        String fullPath = getPath(clusterProperties, relativePath);

        if (runningFile.putIfAbsent(fullPath, new Date().getTime()) == null) {
            try {
                File file = new File(fullPath);
                if (file.exists()) {
                    if (crc32.equals(String.valueOf(FileUtils.checksumCRC32(new File(fullPath))))) {
                        return;
                    }
                }
                HttpUriRequest fileRequest = new HttpGet(fileUrl);
                HttpResponse fileResponse = httpClient.execute(fileRequest);
                if (200 == fileResponse.getStatusLine().getStatusCode()) {
                    if (!crc32.equals(store(clusterProperties, relativePath, fileResponse.getEntity().getContent()))) {
                        throw new RuntimeException("File[" + relativePath + "]'s crc32 doesn't match");
                    }
                    logger.debug("Success to store {}", relativePath);
                } else {
                    throw new RuntimeException("Failed to get file " + relativePath);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                runningFile.remove(fullPath);
            }
        } else {
            logger.debug("{} in sync", fullPath);
        }
    }

    /**
     * 存储文件的底层方法
     *
     * @param clusterProperties
     * @param relativePath
     * @param inputStream
     * @throws Exception
     */
    public static String store(ClusterProperties clusterProperties, String relativePath, InputStream inputStream) throws IOException {
        String fullPath = getPath(clusterProperties, relativePath);
        File file = new File(fullPath);
        if (file.exists()) {
            file.delete();
            //赋予新的iNode
            file = new File(fullPath);
        }
        try {
            FileUtils.copyInputStreamToFile(inputStream, file);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return String.valueOf(FileUtils.checksumCRC32(new File(fullPath)));
    }

    /**
     * 文件上传时metadata的存储方法
     *
     * @param clusterProperties
     * @param fileMetadata
     * @throws IOException
     */
    private static String storeMetadata(ClusterProperties clusterProperties, FileMetadata fileMetadata) throws IOException {
        String fullPath = getMetadataPath(clusterProperties, fileMetadata.getPath());
        File file = new File(fullPath);
        if (file.exists()) {
            file.delete();
            //赋予新的iNode
            file = new File(fullPath);
        }
        FileUtils.write(file, JsonUtil.toJson(fileMetadata, false));
        return String.valueOf(FileUtils.checksumCRC32(new File(fullPath)));
    }

    /**
     * 读取文件到HttpServletResponse的输出流
     *
     * @param clusterProperties
     * @param relativePath
     * @param response
     * @throws IOException
     */
    public static void getFile(ClusterProperties clusterProperties, String relativePath, HttpServletRequest request, HttpServletResponse
            response) throws IOException {
        int buffSize = 256;
        if (relativePath.endsWith(".meta")) {
            response.setStatus(HttpStatus.OK.value());
            response.setHeader(Metadata.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE);

            File metaFile = new File(getMetadataPath(clusterProperties, relativePath));
            response.setHeader(Metadata.CONTENT_LENGTH, String.valueOf(metaFile.length()));
            try (InputStream inputStream = new FileInputStream(metaFile); OutputStream outputStream = response.getOutputStream()) {
                IOUtils.copyLarge(inputStream, outputStream, new byte[buffSize]);
            } catch (ClientAbortException e) {
                logger.warn("Download {} is failing beacause client abort!", relativePath);
            } catch (IOException e) {
                throw e;
            }
        } else {
            FileMetadata fileMetadata = getMetadata(clusterProperties, relativePath);
            //支持范围请求
            response.setHeader(HttpHeaderNames.ACCEPT_RANGES.toString(), "bytes");
            response.setHeader(Metadata.CONTENT_TYPE, fileMetadata.getType());
            response.setHeader(Metadata.CONTENT_MD5, fileMetadata.getMd5());

            String[] urlParts = relativePath.split("/");
            String id = urlParts[urlParts.length - 1].split("\\.")[0];
            String[] idParts = IdMaker.INSTANCE.split(id);
            DateTime dateTime = new DateTime(new Date(Long.valueOf(idParts[3])));
            String lastModify = dateTime.toString(timePattern, Locale.US) + " GMT";
            response.setHeader(HttpHeaderNames.LAST_MODIFIED.toString(), lastModify);
            response.setHeader(CommonConstant.fileName, URLEncoder.encode(fileMetadata.getName(), "UTF-8"));
            for (Map.Entry<String, String> entry : fileMetadata.getExtension().entrySet()) {
                response.setHeader(entry.getKey(), entry.getValue());
            }

            String[] ranges = null;
            //http协议支持单一范围和多重范围查询
            String rangeHeader = request.getHeader(HttpHeaderNames.RANGE.toString());
            if (StringUtils.isNotBlank(rangeHeader) && rangeHeader.contains("bytes=") && rangeHeader.contains("-")) {
                rangeHeader = rangeHeader.trim();
                rangeHeader = rangeHeader.replace("bytes=", "");
                ranges = rangeHeader.split(",");
            }
            String filePath = getPath(clusterProperties, fileMetadata.getPath());
            if (ranges != null) {
                response.setStatus(HttpStatus.PARTIAL_CONTENT.value());
                if (ranges.length == 1) {//单一范围
                    String range = null;
                    Long start = null;
                    Long end = null;
                    if (ranges[0].startsWith("-")) {
                        range = "0" + ranges[0];
                    } else if (ranges[0].endsWith("-")) {
                        range = ranges[0] + (Long.valueOf(fileMetadata.getSize()) - 1);
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
    }

    /**
     * 删除文件
     *
     * @param clusterProperties
     * @param path
     */
    public static void delete(ClusterProperties clusterProperties, String path) {
        String filePath = getPath(clusterProperties, path);
        String metadataPath = getMetadataPath(clusterProperties, path);
        File file = new File(filePath);
        if (file.exists())
            file.delete();
        File metadataFile = new File(metadataPath);
        if (metadataFile.exists())
            metadataFile.delete();
    }

    /**
     * 获取文件在服务器端的绝对路径
     *
     * @param clusterProperties
     * @param relativeFilePath
     * @return
     */
    public static String getPath(ClusterProperties clusterProperties, String relativeFilePath) {
        StringBuilder filePath = new StringBuilder();
        if (!clusterProperties.getStore().getFiledata().getDir().startsWith(File.separator)) {
            filePath.append(FileUtils.getUserDirectoryPath()).append(File.separator);
        } else {
            filePath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        }
        filePath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        filePath.append(relativeFilePath);
        return filePath.toString();
    }

    /**
     * 获取metadata文件在服务器端的绝对地址
     *
     * @param clusterProperties
     * @param relativeFilePath
     * @return
     */
    public static String getMetadataPath(ClusterProperties clusterProperties, String relativeFilePath) {
        if (relativeFilePath.endsWith(".meta"))
            return getPath(clusterProperties, relativeFilePath);
        else
            return getPath(clusterProperties, relativeFilePath) + ".meta";
    }

    /**
     * 获取文件的metadata
     *
     * @param clusterProperties
     * @param relativeFilePath
     * @return
     */
    public static FileMetadata getMetadata(ClusterProperties clusterProperties, String relativeFilePath) throws IOException {
        String metadataJson = FileUtils.readFileToString(new File(getMetadataPath(clusterProperties, relativeFilePath)), "UTF-8");
        FileMetadata fileMetadata = (FileMetadata) JsonUtil.fromJson(metadataJson, FileMetadata.class);
        return fileMetadata;
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
        String filePath = getPath(clusterProperties, fileMetadata.getPath());
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath))) {
            String md5 = DigestUtils.md5Hex(fileInputStream);
            return md5;
        }
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
        String filePath = FileService.getPath(clusterProperties, fileMetadata.getPath());
        Metadata metadata = new Metadata();
        try (FileInputStream inputStream = FileUtils.openInputStream(new File(filePath))) {
            Parser parser = new AutoDetectParser();
            parser.parse(inputStream, new BodyContentHandler(), metadata, new ParseContext());
        }
        return metadata;
    }
}
