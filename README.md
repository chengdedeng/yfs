
[![Join the chat at https://gitter.im/chengdedeng/yfs](https://badges.gitter.im/chengdedeng/yfs.svg)](https://gitter.im/chengdedeng/yfs?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# YFS
YFS是基于[Atomix](https://github.com/atomix/atomix)实现的分布式对象存储，类似于[Swift](https://github.com/openstack/swift)。YFS目标是成为高度可伸缩且安全可靠的对象存储服务，实现海量文件的存储、备份和归档。很多企业都有大量牵涉到商业机密信息的文件需要存储，考虑到安全等诸多因素，一般企业是不愿意放到公有云存储上的，所以就有了YFS的诞生。

YFS在分布式一致性上基于[Raft协议](https://raft.github.io/)，Atomix的前身[Copycat](https://github.com/atomix/copycat)是其中非常优秀的实现，它在[ONF](https://en.wikipedia.org/wiki/Open_Networking_Foundation)的产品中已经得到了充分验证。

# 环境及编译
1. JDK8及以上、maven3
2. mvn package -Dmaven.test.skip=true
3. yfs-gateway/target/yfs-gateway-*.zip和yfs-store/target/yfs-store-*.zip就是部署文件

# Quick Start
为了能够快速搭建YFS整套环境，非常有必要简单阐述下YFS的架构设计，这对于快速上手该项目非常有帮助。

![architecture](https://raw.githubusercontent.com/wiki/chengdedeng/yfs/design.png)

从上图可知YFS由Gateway和分组的Store两部分组成，Gateway主要负责路由、鉴权、流控、安全等非存储功能，Store主要负责存储。每个Group至少由三个Store节点
组成，这三个Store所存储的数据一模一样，也就是说每个文件至少有三个备份。Gateway也至少有三个节点，Store节点会自动上报metadata给Gateway，Gateway根据Store节点上报的信息来调整自己的路由策略。由于使用Raft来解决分布式数据一致性的问题，所以都最少要求三个点。根据2n+1的原则，所以不论是Gateway还是Group中的Store节点在读取写入数据的时候，如果是3个节点，
则最多只能容忍一个节点宕机。
