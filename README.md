
[![Join the chat at https://gitter.im/chengdedeng/yfs](https://badges.gitter.im/chengdedeng/yfs.svg)](https://gitter.im/chengdedeng/yfs?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Yfs
Yfs是基于[Atomix](https://github.com/atomix/atomix)实现的分布式对象存储，类似于[Swift](https://github.com/openstack/swift)。Yfs是一种高度可伸缩且安全可靠的对象存储服务，让您可以存储、备份和归档大量数据。

Yfs提供了一套简单易用的REST API，完整的文档请阅读[Wiki](https://github.com/chengdedeng/yfs/wiki)。

Yfs起源于笔者研究Raft协议的[开源实现](https://raft.github.io/)，Atomix的前身copycat是其中非常优秀的实现，它在[ONF](https://en.wikipedia.org/wiki/Open_Networking_Foundation)的产品中已经得到了充分验证。为了学习并且验证它优良的设计，所以就有了Yfs的诞生。
