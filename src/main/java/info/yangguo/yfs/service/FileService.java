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
        int block = (Hashing.consistentHash(Hashing.murmur3_32().hashBytes(commonsMultipartFile.getName().getBytes()), clusterProperties.getFiledata().getPartition()) + 1);
        fileMetadata.setCreateTime(new Date());
        fileMetadata.setGroup(clusterProperties.getGroup());
        fileMetadata.setName(commonsMultipartFile.getOriginalFilename());
        fileMetadata.setSize(commonsMultipartFile.getSize());
        fileMetadata.setPartition(block);
        fileMetadata.setAddTargetNodes(new HashSet<>());

        long checkSum = store(clusterProperties, fileMetadata, commonsMultipartFile.getInputStream());
        fileMetadata.setCheckSum(checkSum);
        return fileMetadata;
    }

    public static long store(ClusterProperties clusterProperties, FileMetadata fileMetadata, HttpResponse httpResponse) throws IOException {
        return store(clusterProperties, fileMetadata, httpResponse.getEntity().getContent());
    }


    private static long store(ClusterProperties clusterProperties, FileMetadata fileMetadata, InputStream inputStream) throws IOException {
        long checkSum;
        String fileDir = null;
        if (clusterProperties.getFiledata().getDir().startsWith("/")) {
            fileDir = clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getLocal() + "/" + clusterProperties.getGroup() + "/" + fileMetadata.getPartition();
        } else {
            fileDir = FileUtils.getUserDirectoryPath() + "/" + clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getLocal() + "/" + clusterProperties.getGroup() + "/" + fileMetadata.getPartition();
        }
        File dir = new File(fileDir);
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

    public static void getFile(ClusterProperties clusterProperties, String path, HttpServletResponse response) throws IOException {
        String filePath = null;
        if (clusterProperties.getFiledata().getDir().startsWith("/")) {
            filePath = clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getLocal() + "/" + path;
        } else {
            filePath = FileUtils.getUserDirectoryPath() + "/" + clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getLocal() + "/" + path;
        }
        File file = new File(filePath);
        try (InputStream inputStream = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()) {
            IOUtils.copyLarge(inputStream, outputStream);
        } catch (IOException e) {
            throw e;
        }
    }
}
