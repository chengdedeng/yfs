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
package info.yangguo.yfs.util;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;

import java.math.BigInteger;
import java.util.List;

/**
 * @author:杨果
 * @date:2017/4/18 上午11:17
 * <p>
 * Description:
 * <p>
 * 权重轮询调度算法(WeightedRound-RobinScheduling)-Java实现
 */
public class WeightedRoundRobinScheduling {
    private int currentIndex = -1;// 上一次选择的服务器
    private int currentWeight = 0;// 当前调度的权值
    public List<Server> healthilyServers = Lists.newArrayList(); //健康服务器集合

    /**
     * 返回最大公约数
     */
    private int gcd(int a, int b) {
        BigInteger b1 = new BigInteger(String.valueOf(a));
        BigInteger b2 = new BigInteger(String.valueOf(b));
        BigInteger gcd = b1.gcd(b2);
        return gcd.intValue();
    }


    /**
     * 返回所有服务器权重的最大公约数
     */
    private int getGCDForServers(List<Server> serverList) {
        int w = 0;
        for (int i = 0, len = serverList.size(); i < len - 1; i++) {
            if (w == 0) {
                w = gcd(serverList.get(i).weight, serverList.get(i + 1).weight);
            } else {
                w = gcd(w, serverList.get(i + 1).weight);
            }
        }
        return w;
    }

    /**
     * 返回所有服务器中的最大权重
     */
    private int getMaxWeightForServers(List<Server> serverList) {
        int w = 0;
        for (int i = 0, len = serverList.size(); i < len - 1; i++) {
            if (w == 0) {
                w = Math.max(serverList.get(i).weight, serverList.get(i + 1).weight);
            } else {
                w = Math.max(w, serverList.get(i + 1).weight);
            }
        }
        return w;
    }

    /**
     * 算法流程： 假设有一组服务器 S = {S0, S1, …, Sn-1} 有相应的权重，变量currentIndex表示上次选择的服务器
     * 权值currentWeight初始化为0，currentIndex初始化为-1 ，当第一次的时候返回 权值取最大的那个服务器， 通过权重的不断递减 寻找
     * 适合的服务器返回，直到轮询结束，权值返回为0
     */
    public Server getServer() {
        if (healthilyServers.size() == 0) {
            return null;
        } else if (healthilyServers.size() == 1) {
            return healthilyServers.get(0);
        } else {
            while (true) {
                currentIndex = (currentIndex + 1) % healthilyServers.size();
                if (currentIndex == 0) {
                    currentWeight = currentWeight - getGCDForServers(healthilyServers);
                    if (currentWeight <= 0) {
                        currentWeight = getMaxWeightForServers(healthilyServers);
                        if (currentWeight == 0)
                            return null;
                    }
                }
                if (healthilyServers.get(currentIndex).weight >= currentWeight) {
                    return healthilyServers.get(currentIndex);
                }
            }
        }
    }

    @Getter
    @Setter
    public static class Server {
        String group;
        String id;
        String ip;
        int port;
        int weight;

        public Server(String group, String id, String ip, int port, int weight) {
            super();
            this.group = group;
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.weight = weight;
        }
    }
}
