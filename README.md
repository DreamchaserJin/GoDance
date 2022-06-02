GoDance是一款用go语言编写的分布式搜索引擎，同时也是一款分布式文档数据库。支持分布式搜索以及分布式存储功能，对外提供restful Api接口来操作GoDance。

GoDance整体采用主从架构，实现了Raft算法来保证元数据的一致，并有一系列机制比如tranlog、分片存储、分片迁移、故障转移等来保证集群的高可用和高拓展性；在存储方面，利用了段的思想来提升搜索和插入的性能，同时支持正排索引（B+树）和倒排索引（BST和MAP）来提升搜索能力，并自己实现了相应的数据结构和Linux系统上的持久化机制（MMAP）；在路由方面，根据索引内存负载率和机器配置会优先路由效率高的分片节点，同时使用了rpcx来进行rpc调用；在搜索方面，实现了TF-IDF等搜索算法，并利用其实现了相关度搜索。
##目录模块
- cmd 存放项目要编译构建的可执行文件对应的 main 包的源文件
- cluster 分布式模块，实现分布式集群能力，分布式搜索能力
- engine 引擎层，负责执行计划的生成，执行下层提供服务，为上层提供相对抽象的服务

- index 索引模块，对外提供正排索引和倒排索引的构建等相关操作(分词也在这层)
-- field 字段模块，提供字段相关的操作
-- segment 段模块，提供分段存储能力，实现段合并、索引缓存（要实现内存负载率的机制）等功能
- search 搜索算法模块，提供相关度搜索算法
- shard 分片模块，该模块主要提供分片功能，包括分片转移等等
- store 底层存储模块，该模块主要与os文件存储进行交互，提供数据写入文件的相关操作
- utils 工具包，比如对分词器或者其他工具的封装
- web web服务模块，对外提供相对应的restful接口服务

![搜索引擎分层架构图](https://cdn.jsdelivr.net/gh/BestDreamChaser/picture/img/202205211111001.png)

