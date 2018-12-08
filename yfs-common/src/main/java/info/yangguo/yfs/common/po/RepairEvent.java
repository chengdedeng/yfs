package info.yangguo.yfs.common.po;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RepairEvent implements Serializable {
    /**
     * 修复所需信息
     */
    HashMap<String, HashSet<String>> repairInfo;
}
