package info.yangguo.yfs.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.yangguo.yfs.config.ClusterProperties;
import info.yangguo.yfs.config.YfsConfig;
import info.yangguo.yfs.dto.Result;
import info.yangguo.yfs.dto.ResultCode;
import info.yangguo.yfs.po.FileMetadata;
import info.yangguo.yfs.service.MetadataService;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.utils.time.Versioned;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping(value = "${yfs.group}/metadata")
public class MetadataController extends BaseController {
    @Autowired
    private ClusterProperties clusterProperties;

    @ApiOperation(value = "get all file metadata by paging query")
    @RequestMapping(value = "file", method = {RequestMethod.GET})
    public void getAllFileMetadata(@RequestParam("pageSize") Integer pageSize, @RequestParam(value = "index", required = false) String index, HttpServletResponse response) {
        Result result = new Result<>();
        Map<String, Object> values = Maps.newHashMap();
        try {
            ConsistentTreeMap<FileMetadata> consistentMap = YfsConfig.fileMetadataConsistentMap;
            values.put("count", consistentMap.size());
            values.put("pageSize", pageSize);
            List<FileMetadata> metadataList = Lists.newArrayList();
            if (index == null) {
                index = consistentMap.firstKey();
            }

            for (int i = 0; i < pageSize; i++) {
                if (index == null)
                    break;
                Versioned<FileMetadata> versioned = consistentMap.get(index);
                if (versioned != null) {
                    metadataList.add(versioned.value());
                    index = consistentMap.higherKey(index);
                }
            }
            values.put("value", metadataList);
            values.put("index", index);

            result.setValue(values);
            result.setCode(ResultCode.C200.getCode());
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        outputResult(response, result);
    }

    @ApiOperation(value = "get metadata for one file")
    @RequestMapping(value = "file/{partition}/{name:.+}", method = {RequestMethod.GET})
    public void getOneFileMetadata(@PathVariable Integer partition, @PathVariable String name, HttpServletResponse response) {
        Result result = new Result<>();
        try {
            String key = MetadataService.getKey(clusterProperties.getGroup(), partition, name);
            FileMetadata metadata = YfsConfig.fileMetadataConsistentMap.get(key).value();
            result.setValue(metadata);
            result.setCode(ResultCode.C200.getCode());
        } catch (Exception e) {
            result.setCode(ResultCode.C500.getCode());
            result.setValue(ResultCode.C500.getDesc());
        }
        outputResult(response, result);
    }
}
