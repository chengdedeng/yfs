package info.yangguo.yfs;

import info.yangguo.yfs.common.po.StoreInfo;
import info.yangguo.yfs.common.utils.JsonUtil;
import info.yangguo.yfs.util.WeightedRoundRobinScheduling;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author:杨果
 * @date:2017/5/17 下午2:16
 * <p>
 * Description:
 */
public class WeightedRoundRobinSchedulingTest {
    @Test
    public void test1() {
        StoreInfo storeInfo1=new StoreInfo("group1","s1","192.168.1.1",8080,9090,7070,0L,null,null);
        StoreInfo storeInfo2=new StoreInfo("group1","s2","192.168.1.2",8080,9090,7070,0L,null,null);
        StoreInfo storeInfo3=new StoreInfo("group1","s3","192.168.1.3",8080,9090,7070,0L,null,null);
        StoreInfo storeInfo4=new StoreInfo("group2","s1","192.168.1.4",8080,9090,7070,0L,null,null);
        StoreInfo storeInfo5=new StoreInfo("group2","s2","192.168.1.5",8080,9090,7070,0L,null,null);
        StoreInfo storeInfo6=new StoreInfo("group2","s3","192.168.1.6",8080,9090,7070,0L,null,null);
        WeightedRoundRobinScheduling.Server s1 = new WeightedRoundRobinScheduling.Server(storeInfo1, 1);
        WeightedRoundRobinScheduling.Server s2 = new WeightedRoundRobinScheduling.Server(storeInfo2, 1);
        WeightedRoundRobinScheduling.Server s3 = new WeightedRoundRobinScheduling.Server(storeInfo3, 1);
        WeightedRoundRobinScheduling.Server s4 = new WeightedRoundRobinScheduling.Server(storeInfo4, 1);
        WeightedRoundRobinScheduling.Server s5 = new WeightedRoundRobinScheduling.Server(storeInfo5, 1);
        WeightedRoundRobinScheduling.Server s6 = new WeightedRoundRobinScheduling.Server(storeInfo6, 1);
        List<WeightedRoundRobinScheduling.Server> serverList = new ArrayList<>();
        serverList.add(s1);
        serverList.add(s2);
        serverList.add(s3);
        serverList.add(s4);
        serverList.add(s5);
        serverList.add(s6);
        WeightedRoundRobinScheduling obj = new WeightedRoundRobinScheduling();
        obj.healthilyServers.addAll(serverList);

        Map<String, Integer> countResult = new HashMap<>();

        for (int i = 0; i < 100; i++) {
            WeightedRoundRobinScheduling.Server s = obj.getServer();
            String log = JsonUtil.toJson(s,false);
            if (countResult.containsKey(log)) {
                countResult.put(log, countResult.get(log) + 1);
            } else {
                countResult.put(log, 1);
            }
            System.out.println(log);
        }

        for (Map.Entry<String, Integer> map : countResult.entrySet()) {
            System.out.println("服务器 " + map.getKey() + " 请求次数： " + map.getValue());
        }

        countResult = new HashMap<>();
        obj.healthilyServers.remove(0);
        for (int i = 0; i < 100; i++) {
            WeightedRoundRobinScheduling.Server s = obj.getServer();
            String log = JsonUtil.toJson(s,false);
            if (countResult.containsKey(log)) {
                countResult.put(log, countResult.get(log) + 1);
            } else {
                countResult.put(log, 1);
            }
            System.out.println(log);
        }

        for (Map.Entry<String, Integer> map : countResult.entrySet()) {
            System.out.println("服务器 " + map.getKey() + " 请求次数： " + map.getValue());
        }
    }
}
