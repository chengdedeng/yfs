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
import info.yangguo.yfs.common.utils.IdMaker;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.StandardHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileService {
    private static Logger logger = LoggerFactory.getLogger(FileService.class);
    public static Map<String, Long> runningFile = new ConcurrentHashMap<>();
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
    public static Pair<String, FileEvent> store(ClusterProperties clusterProperties, CommonsMultipartFile commonsMultipartFile, HttpServletRequest httpServletRequest) throws IOException {
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

        Map<String, String> systemMetas = Maps.newHashMap();
        Map<String, String> userMetas = Maps.newHashMap();
        Set<String> standardHeaderNames = Arrays.stream(StandardHeaders.class.getFields()).map(field -> field.getName()).collect(Collectors.toSet());
        Enumeration headerNames = httpServletRequest.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = ((String) headerNames.nextElement()).toLowerCase();
            if (standardHeaderNames.contains(headerName)) {
                String headerValue = httpServletRequest.getHeader(headerName);
                userMetas.put(headerName, headerValue);
            } else if (headerName.startsWith(CommonConstant.xHeaderPrefix)) {
                String headerValue = httpServletRequest.getHeader(headerName);
                userMetas.put(headerName, headerValue);
            }
        }

        try {
            runningFile.putIfAbsent(relativePath, new Date().getTime());
            store(clusterProperties, relativePath, commonsMultipartFile.getInputStream(), systemMetas, userMetas);
            String clientMd5 = httpServletRequest.getHeader(HttpHeaderNames.CONTENT_MD5.toString());
            if (StringUtils.isNotBlank(clientMd5)) {
                String serverMd5 = verifyFileByMd5(clusterProperties, relativePath);
                if (clientMd5 != null) {
                    if (!clientMd5.equals(serverMd5)) {
                        throw new IOException("md5 does't match");
                    }
                }
            }
        } catch (IOException e) {
            delete(clusterProperties, relativePath);
            throw e;
        } finally {
            runningFile.remove(relativePath);
        }
        return new ImmutablePair<>(relativePath, fileEvent);
    }


    /**
     * 同步时文件存储方法
     *
     * @param clusterProperties
     * @param relativePath
     * @param fileUrl
     */
    public static void store(ClusterProperties clusterProperties, String relativePath, String fileUrl) throws IOException {
        String fullPath = getFullPath(clusterProperties, relativePath);

        if (runningFile.putIfAbsent(relativePath, new Date().getTime()) == null) {
            try {
                File file = new File(fullPath);
                if (file.exists()) {
                    return;
                }
                HttpUriRequest fileRequest = new HttpGet(fileUrl);
                HttpResponse fileResponse = httpClient.execute(fileRequest);

                Map<String, String> systemMetas = Maps.newHashMap();
                Map<String, String> userMetas = Maps.newHashMap();
                Set<String> standardHeaderNames = Arrays.stream(StandardHeaders.class.getFields()).map(field -> field.getName()).collect(Collectors.toSet());
                Arrays.stream(fileResponse.getAllHeaders()).forEach(header -> {
                    String headerName = header.getName().toLowerCase();
                    if (standardHeaderNames.contains(headerName)) {
                        String headerValue = header.getValue();
                        userMetas.put(headerName, headerValue);
                    } else if (headerName.startsWith(CommonConstant.xHeaderPrefix)) {
                        String headerValue = header.getValue();
                        userMetas.put(headerName, headerValue);
                    } else if (headerName.equals(CommonConstant.CRC32)) {
                        String headerValue = header.getValue();
                        userMetas.put(CommonConstant.CRC32, headerValue);
                    }
                });

                if (200 == fileResponse.getStatusLine().getStatusCode()) {
                    store(clusterProperties, relativePath, fileResponse.getEntity().getContent(), systemMetas, userMetas);
                    logger.debug("Success to store {}", relativePath);
                } else {
                    throw new RuntimeException("Failed to get file " + relativePath);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                runningFile.remove(relativePath);
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
    public static void store(ClusterProperties clusterProperties, String relativePath, InputStream inputStream, Map<String, String> systemMetas, Map<String, String> userMetas) throws IOException {
        String fullPath = getFullPath(clusterProperties, relativePath);
        File file = new File(fullPath);
        if (file.exists()) {
            file.delete();
            //赋予新的iNode
            file = new File(fullPath);
        }
        FileUtils.copyInputStreamToFile(inputStream, file);
        String crc32 = String.valueOf(FileUtils.checksumCRC32(file));
        if (systemMetas.containsKey(CommonConstant.CRC32)) {
            if (!systemMetas.get(CommonConstant.CRC32).equals(crc32)) {
                throw new RuntimeException("File[" + relativePath + "]'s crc32 doesn't match");
            }
        } else {
            systemMetas.put(CommonConstant.CRC32, crc32);
        }
        FileAttributes.setXattr(systemMetas, fullPath);
        FileAttributes.setXattr(userMetas, fullPath);
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

        //支持范围请求
        response.setHeader(HttpHeaderNames.ACCEPT_RANGES.toString(), "bytes");
        //添加xattr
        FileAttributes.getAllXattr(FileService.getFullPath(clusterProperties, relativePath))
                .entrySet()
                .stream()
                .forEach(entry -> {
                    response.setHeader(entry.getKey(), entry.getValue());
                });

        String[] ranges = null;
        //http协议支持单一范围和多重范围查询
        String rangeHeader = request.getHeader(HttpHeaderNames.RANGE.toString());
        if (StringUtils.isNotBlank(rangeHeader) && rangeHeader.contains("bytes=") && rangeHeader.contains("-")) {
            rangeHeader = rangeHeader.trim();
            rangeHeader = rangeHeader.replace("bytes=", "");
            ranges = rangeHeader.split(",");
        }
        String filePath = getFullPath(clusterProperties, relativePath);
        BasicFileAttributes basicFileAttributes = FileAttributes.getBasicAttrs(filePath);
        if (ranges != null) {
            response.setStatus(HttpStatus.PARTIAL_CONTENT.value());
            if (ranges.length == 1) {//单一范围
                String range = null;
                Long start = null;
                Long end = null;
                if (ranges[0].startsWith("-")) {
                    range = "0" + ranges[0];
                } else if (ranges[0].endsWith("-")) {
                    range = ranges[0] + (basicFileAttributes.size() - 1);
                }
                String[] rangeInfo = range.split("-");
                start = Long.valueOf(rangeInfo[0]);
                end = Long.valueOf(rangeInfo[1]);

                if (start < 0 || end < 0 || start.longValue() == end.longValue()) {
                    throw new RuntimeException("range is mistake");
                } else {
                    Long contentLength = end - start + 1;
                    response.setHeader(HttpHeaderNames.CONTENT_RANGE.toString(), "bytes " + start + "-" + end + "/" + basicFileAttributes.size());
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
                        logger.warn("Download {} is failing beacause client abort!", relativePath);
                    } catch (IOException e) {
                        throw e;
                    }
                }
            } else {//多重范围暂时不支持
            }
        } else {//client没有进行范围查询
            response.setStatus(HttpStatus.OK.value());
            response.setHeader(Metadata.CONTENT_LENGTH, String.valueOf(basicFileAttributes.size()));

            File file = new File(filePath);
            try (InputStream inputStream = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()) {
                IOUtils.copyLarge(inputStream, outputStream, new byte[buffSize]);
            } catch (ClientAbortException e) {
                logger.warn("Download {} is failing beacause client abort!", relativePath);
            } catch (IOException e) {
                throw e;
            }
        }
    }

    /**
     * 删除原文件
     *
     * @param clusterProperties
     * @param key
     */
    public static void delete(ClusterProperties clusterProperties, String key) {
        if (!runningFile.containsKey(key)) {
            String fullPath = getFullPath(clusterProperties, key);
            File file = new File(fullPath);
            if (file.exists())
                file.delete();
        }
    }

    /**
     * 获取文件在服务器端的绝对路径
     *
     * @param clusterProperties
     * @param relativeFilePath
     * @return
     */
    public static String getFullPath(ClusterProperties clusterProperties, String relativeFilePath) {
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
     * 获取文件在服务器端的相对路径
     *
     * @param clusterProperties
     * @param absolutePath
     * @return
     */
    public static String getRelativePath(ClusterProperties clusterProperties, String absolutePath) {
        StringBuilder parentPath = new StringBuilder();
        if (!clusterProperties.getStore().getFiledata().getDir().startsWith(File.separator)) {
            parentPath.append(FileUtils.getUserDirectoryPath()).append(File.separator);
        } else {
            parentPath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        }
        parentPath.append(clusterProperties.getStore().getFiledata().getDir()).append(File.separator);
        return absolutePath.replace(parentPath.toString(), "");
    }

    /**
     * 获取文件的MD5值
     *
     * @param clusterProperties
     * @param relativeFilePath
     * @return
     * @throws IOException
     */
    public static String verifyFileByMd5(ClusterProperties clusterProperties, String relativeFilePath) throws IOException {
        String filePath = getFullPath(clusterProperties, relativeFilePath);
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath))) {
            String md5 = DigestUtils.md5Hex(fileInputStream);
            return md5;
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param path
     */
    public static boolean checkExist(String path) {
        File file = new File(path);
        if (file.exists())
            return true;
        else
            return false;
    }
}
