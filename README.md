
[![Join the chat at https://gitter.im/chengdedeng/yfs](https://badges.gitter.im/chengdedeng/yfs.svg)](https://gitter.im/chengdedeng/yfs?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# YFS
![architecture](https://raw.githubusercontent.com/wiki/chengdedeng/yfs/design.png)

YFS由Gateway和Group组成，Gateway主要负责路由、鉴权、流控、安全等非存储功能，Group主要负责数据存储。
YFS采用对称式架构，没有专门的元数据管理节点，元数据和文件都分散在多个Group（存储单元）。为了实现元数据的分布式一致性和分布式协调，YFS选择了[Atomix](https://github.com/atomix/atomix)，由于
Atomix不但实现了[Raft](https://raft.github.io/)，而且提供了非常丰富的高层抽象。基于Raft协议、高可用以及文件的安全等诸多因素，所以Gateway、Group建议至少3个节点。

Group中的Store会实时上报心跳给Gateway，Gateway根据上报的信息实时修改自己的路由策略。为了保证写入的数据安全，YFS认为超半数节点写入完成才算写入成功，如果采用常规的3副本模式，那么至少2个节点写入才会返回成功。
为了应对硬件、软件、运维等多方面的威胁，YFS实现了端到端的CRC保护，能够及时检查和快速恢复异常数据。


# 环境及编译
1. JDK8及以上、maven3、文件系统需要支持xattr
2. mvn package -Dmaven.test.skip=true
3. yfs-gateway/target/yfs-gateway-*.zip和yfs-store/target/yfs-store-*.zip就是部署文件