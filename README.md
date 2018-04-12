
[![Join the chat at https://gitter.im/chengdedeng/yfs](https://badges.gitter.im/chengdedeng/yfs.svg)](https://gitter.im/chengdedeng/yfs?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Yfs
Yfs是基于[Atomix](https://github.com/atomix/atomix)实现的分布式对象存储，类似于[Swift](https://github.com/openstack/swift)。Yfs是一种高度可伸缩且安全可靠的对象存储服务，让您可以存储、备份和归档大量数据。

Yfs起源于笔者研究Raft协议的[开源实现](https://raft.github.io/)，Atomix的前身[Copycat](https://github.com/atomix/copycat)是其中非常优秀的实现，它在[ONF](https://en.wikipedia.org/wiki/Open_Networking_Foundation)的产品中已经得到了充分验证。为了学习并且验证它优良的设计，所以就有了Yfs的诞生。

Yfs提供了一套简单易用的REST API，完整的文档请阅读[Wiki](https://github.com/chengdedeng/yfs/wiki)。

# Quick Start
为了能够快速搭建YFS整套环境，非常有必要简单阐述下YFS的架构设计，这对于快速上手该项目非常有帮助。

![architecture](https://raw.githubusercontent.com/wiki/chengdedeng/yfs/design.png)

从上图可知YFS由Gateway和分组的Store两部分组成，Gateway主要负责路由、鉴权、流控、安全等非存储功能，Store主要负责存储。每个Group至少由三个Store节点
组成，这三个Store所存储的数据一模一样，也就是说每个文件至少有三个备份。Gateway也至少有三个节点，Store节点会自动上报metadata给Gateway，Gateway根据
Store节点上报的信息来调整自己的路由策略。由于使用Raft来解决分布式数据一致性的问题，所以都最少要求三个点。根据2n+1的原则，所以不论是Gateway还是Group
在写入数据的时候，最多只能允许一个节点宕机；当然读只要还有一个点，就可以成功。



