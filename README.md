# MIT 6.824 Spring 2016 学习笔记



##写在最前

​	博客捣鼓过Hexo，也用Django搭过，最终都在没完成好之前被其他事情打断了，也就懒得再弄。所以不如回归最初的需求——记录下自己的学习历程，暂且先就这么写着😉

​	这是我第一次认认真真的写笔记，一是课程并没有系统的书籍可以参考而是以读paper为主，所以需要记录大量的信息，二是想通过此来锻炼下自己的写作表达能力（语文功底实在差劲😭）

​	一直对分布式系统比较感兴趣，但总没有时间去接触，假期闲了于是乎学习下分布式系统的经典课程[MIT 6.824](http://nil.csail.mit.edu/6.824/2016/)。课程网站上有schedule可以看到每一节课的note和assignment以及需要读的paper，因为以读paper和做lab为主，所以没有课件和视频（youtube有学生录制的2015年的课程视频，没有字幕英语渣着实听不懂）并没有多大影响。2012及以前使用的是C++，之后的lab都基于Go实现，虽说要重新学一门新语言，最开始肯定没有C++用起来顺手，但随着学习深入和熟悉Go的许多特性也让人从代码的细枝末节中解脱出来，其`channel`特性更是使得异步通信变得异常简单。

​	最后不得不说从人家Lab的设计和文档的详尽程度上可以看出来非常用心，还有详尽周全的test，对比下自己学校的课程实验，想想都辛酸呐😷



## Week 1

### MapReduce

​	`键值对 <key, value>`

​	`map`、`reduce`为函数式编程中两个常用函数。`map`函数将输入的`key`值对通过某种函数关系产生中间键值对，之后将多个`map`产生的中间键值队以`key`分组形成`value`集合，`reduce`函数以`key`值和相应的`value`集合为输入将`value`集合进行合并产生最终结果（一般一个输出值或无输出值）。

​	一个非常经典的例子wordcount：`map`为一个文档中的每个单词产生`<单词，个数1>`的中间键值对，相同的单词通过`reduce`将个数相加，得到这个单词的总个数。同理多个`reduce`函数就完成了所有单词个数的统计

```C++
map(String key, String value):
	// key: document name
	// value: document contents
	for each word w in value:
		EmitIntermediate(w, "1");

reduce(String key, List values):
	// key: a word
	// values: a list of counts
	int result = 0;
	for each v in values:
		result += ParseInt(v);
	Emit(AsString(result));
```

#### 执行步骤

![](./doc/mapreduce-execution_overview.png)

1.  输入文件分割成*M*块（通常为64MB）对应*M*个`map`任务，运行的mapreduce程序会复制到每个集群的节点上
2.  其中有个`master`节点，负责控制分发*M*个`map`任务和*R*个`reduce`任务到空闲的工作节点上
3.  执行`map`任务的节点，读入相应的输入文件块经过处理得到中间键值对，并缓存在内存中
4.  周期性的将缓存在内存的中间键值对写入本地磁盘，并根据分割函数`(e.g., hash(key) mod R)`分成*R*份，然后将这些信息的位置传送回`master`节点
5.  `master`节点将中间键值对的位置告知执行`reduce`任务的节点，`reduce`节点使用`RPC(Remote Procedure Call)`得到的所有中间键值对排序使得同一`key`的键值对聚集在一起（需要排序是因为有可能多个`key`值都对应h同一个`reduce`任务）
6.  `reduce`节点将排序过的中间键值对通过`reduce`函数得到输出
7.  等待所有任务结束，`master`返回结果到用户程序




### PRC

​	参阅Go的[RPC](https://golang.org/pkg/net/rpc/)文档以及Effective Go




## Week2

### GFS(Google File System)

![](./doc/gfs-architecture.png)

#### 结构

-   `master`
    -   维护整个文件系统的元数据
    -   管理系统性操作
        -   chunk lease management
        -   garbage collection of orphaned chunks
        -   chunk migration between chunkservers
    -   周期性地与`chunkserver`通过`HeartBeat信息`发送指令或者收集状态
-   `chunkserver`保存数据，一个文件块会复制存放在多个不同的`chunkserver`上
-   `client`面向应用程序提供API，从`master`获取元数据，但直接与`chunkserver`进行数据通信

`client`和`chunkserver`均不缓存数据，但`client`会缓存元数据以减少与`master`的交互



#### 块大小

使用大文件块大小——64MB，使用lazy space allocation解决内部碎片问题

好处：

-   减少`client`与`master`的交互
-   通过保持TCP长连接减少网络负载
-   允许`master`将所有元信息保存在内存之中

缺点：

-   如果很多`client`都访问同一个小文件（只占用很少的文件块，也许只占用一个），会成为`hot spot`（论文中指出，在实际中应用更多的操作是大型文件的连续读取，所以`hot spot`并不算是大问题）





#### 元数据

1.  the file and chunk namespaces
2.  the maping from files to chuncks
3.  the current locations of chunks

前两种通过log持久化在`master`并同步到远程主机

第三种不持久化在`master`上，而是在`master`启动时或者有新的`chunkserver`加入集群时询问这些信息



####操作日志

记录重要元数据的改变，不仅用于持久化还用于操作定序

在本地和远程持久化后才能应答客户端

`master`每次通过操作日志恢复文件系统的状态，为了减少启动时间就必须减小日志大小，所以在日志超过一定大小时就建立`checkpoint（类似压缩的B-tree，可以直接映射到内存上从而达到快速恢复）`，而日志只需记录`checkpoint`之后的操作



#### 一致性模型



​                                                                                                                                

