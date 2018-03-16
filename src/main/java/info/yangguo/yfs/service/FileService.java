package info.yangguo.yfs.service;

import com.google.common.hash.Hashing;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.po.FileMetadata;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Date;

@Service
public class FileService {
    private static Logger logger = LoggerFactory.getLogger(FileService.class);
    @Autowired
    private ClusterProperties clusterProperties;

    public FileMetadata store(CommonsMultipartFile commonsMultipartFile) throws IOException {
        FileMetadata fileMetadata = new FileMetadata();
        int block = (Hashing.consistentHash(Hashing.murmur3_32().hashBytes(commonsMultipartFile.getName().getBytes()), clusterProperties.getFiledata().getPartition()) + 1);
        fileMetadata.setCreateTime(new Date());
        fileMetadata.setGroup(clusterProperties.getGroup());
        fileMetadata.setName(commonsMultipartFile.getOriginalFilename());
        fileMetadata.setSize(commonsMultipartFile.getSize());
        fileMetadata.setPartition(block);

        String fileDir = null;
        if (clusterProperties.getFiledata().getDir().startsWith("/")) {
            fileDir = clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getGroup() + "/" + block;
        } else {
            fileDir = FileUtils.getUserDirectoryPath() + "/" + clusterProperties.getFiledata().getDir() + "/" + clusterProperties.getGroup() + "/" + block;
        }
        File dir = new File(fileDir);
        if (!dir.exists()) {
            FileUtils.forceMkdir(dir);
        }
        File file = new File(fileDir + "/" + commonsMultipartFile.getOriginalFilename());
        file.deleteOnExit();

        try (InputStream inputStream = commonsMultipartFile.getInputStream()) {
            FileUtils.copyInputStreamToFile(inputStream, file);
            fileMetadata.setCheckSum(FileUtils.checksumCRC32(file));
        } catch (Exception e) {
            file.deleteOnExit();
        }
        return fileMetadata;
    }

    public void getFile(String path, HttpServletResponse response) {
        String filePath = null;
        if (clusterProperties.getFiledata().getDir().startsWith("/")) {
            filePath = clusterProperties.getFiledata().getDir() + "/" + path;
        } else {
            filePath = FileUtils.getUserDirectoryPath() +"/"+ clusterProperties.getFiledata().getDir() + "/" + path;
        }
        File file = new File(filePath);
        try (InputStream inputStream = new FileInputStream(file); OutputStream outputStream = response.getOutputStream()) {
            IOUtils.copyLarge(inputStream, outputStream);
        } catch (Exception e) {
            logger.error("file:{}下载失败", filePath);
        }
    }

    public void delete(FileMetadata fileMetadata) {
    }
}
