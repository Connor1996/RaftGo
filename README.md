# MIT 6.824 Spring 2016 学习笔记



##写在最前

​	博客捣鼓过Hexo，也用Django搭过，最终都在没完成好之前被其他事情打断了，也就懒得再弄。所以不如回归最初的需求——记录下自己的学习历程，暂且先就这么写着😉

​	这是我第一次认认真真的写笔记，一是课程并没有系统的书籍可以参考而是以读paper为主，所以需要记录大量的信息，二是想通过此来锻炼下自己的写作表达能力（语文功底实在差劲😭）

​	一直对分布式系统比较感兴趣，但总没有时间去接触，假期闲了于是乎学习下分布式系统的经典课程[MIT 6.824](http://nil.csail.mit.edu/6.824/2016/)。课程网站上有schedule可以看到每一节课的note和assignment以及需要读的paper，因为以读paper和做lab为主，所以没有课件和视频（youtube有学生录制的2015年的课程视频，没有字幕英语渣着实听不懂）并没有多大影响。2012及以前使用的是C++，之后的lab都基于Go实现，虽说要重新学一门新语言，最开始肯定没有C++用起来顺手，但随着学习深入和熟悉Go的许多特性也让人从代码的细枝末节中解脱出来，其`channel`特性更是使得异步通信变得异常简单。

​	最后不得不说从人家Lab的设计和文档的详尽程度上可以看出来非常用心，还有详尽周全的test，对比下自己学校的课程实验，想想都辛酸呐😷



## MapReduce(Week 1)

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

### 执行步骤

![](./doc/mapreduce-execution_overview.png)

1.  输入文件分割成*M*块（通常为64MB）对应*M*个`map`任务，运行的mapreduce程序会复制到每个集群的节点上
2.  其中有个`master`节点，负责控制分发*M*个`map`任务和*R*个`reduce`任务到空闲的工作节点上
3.  执行`map`任务的节点，读入相应的输入文件块经过处理得到中间键值对，并缓存在内存中
4.  周期性的将缓存在内存的中间键值对写入本地磁盘，并根据分割函数`(e.g., hash(key) mod R)`分成*R*份，然后将这些信息的位置传送回`master`节点
5.  `master`节点将中间键值对的位置告知执行`reduce`任务的节点，`reduce`节点使用`RPC(Remote Procedure Call)`得到的所有中间键值对排序使得同一`key`的键值对聚集在一起（需要排序是因为有可能多个`key`值都对应h同一个`reduce`任务）
6.  `reduce`节点将排序过的中间键值对通过`reduce`函数得到输出
7.  等待所有任务结束，`master`返回结果到用户程序




## PRC(Week 1)

​	参阅Go的[RPC](https://golang.org/pkg/net/rpc/)文档以及Effective Go




## GFS(Google File System)(Week 2)

![](./doc/gfs-architecture.png)

### 结构

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



### 块大小

使用大文件块大小——64MB，使用lazy space allocation解决内部碎片问题

好处：

-   减少`client`与`master`的交互
-   通过保持TCP长连接减少网络负载
-   允许`master`将所有元信息保存在内存之中

缺点：

-   如果很多`client`都访问同一个小文件（只占用很少的文件块，也许只占用一个），会成为`hot spot`（论文中指出，在实际中应用更多的操作是大型文件的连续读取，所以`hot spot`并不算是大问题）





### 元数据

1.  the file and chunk namespaces
2.  the maping from files to chuncks
3.  the current locations of chunks

前两种通过log持久化在`master`并同步到远程主机

第三种不持久化在`master`上，而是在`master`启动时或者有新的`chunkserver`加入集群时询问这些信息



###操作日志

记录重要元数据的改变，不仅用于持久化还用于操作定序

在本地和远程操作日志持久化后才能应答客户端

`master`每次通过操作日志恢复文件系统的状态，为了减少启动时间就必须减小日志大小，所以在日志超过一定大小时就建立`checkpoint（类似压缩的B-tree，可以直接映射到内存上从而达到快速恢复）`，而日志只需记录`checkpoint`之后的操作



### 一致性模型

![](./doc/gfs-file_region_state_after_mutation.png)

上图为在写操作或附加操作之后区域的状态

`consitent` ：所有的`client`看到的数据都一样，无论读哪一份拷贝

`defined` ：区域是`consistent`的，并且`client`能看到实际的数据改变（**write操作是由用户指定offset，那在并发的情况下，就有可能导致用户采用它觉得合理的offset，而实际上会导致并发写入的数据相互混合，这样我们就无从得知哪些操作分别写入了哪部分数据**）



### 系统交互

设计目标：最小化`master`的参与

#### lease and mutation order

`master`将租约（lease）授予一个`chunck`的副本，使其为主副本。在对于`chunck`改变数据时，主副本挑选一个顺序执行改变，其他副本按照主副本的顺序改变，以保持一致性

租约初始值为60s，过期无效，在`HeartBeat`信息中可以续约

`master`在某种情况下可能提前撤销租约（e.g. when the master wants to disable mutations on a file that is being renamed）



```
未完待续。。。
```



## Fault-Tolerant Virtual Machines(Week 2)

简单的说，就是使用虚拟机技术实现GFS部分中对`master`的远程备份服务器

通过从一个初始状态开始，经过相同的输入顺序确保主备服务器达到同步，但在同步时，对于**不确定操作**（如读取时钟，异常）就必须发送额外的信息。而这些额外信息在虚拟机上可以非常方便的获取到

缺点：不支持多处理器（性能表现差劲，因为对于共享内存的访问几乎都是不确定操作）

主VM将所有接受到的输入通过网络连接——`logging channel`发送给备份VM



### 基本设计

#### Deterministic Replay Implentation

挑战：

1.  正确地获取所有输入和不确定性
2.  正确应用输入和不确定性操作到备份虚拟VM上
3.  不降低性能

记录所有操作到日志中。对于不确定操作，还将某一指令引起的事件记录下来，在重现时也产生该事件在对应的指令。

#### FT 协议

将操作记录不存储在硬盘里，而是直接通过`logging channel`发送给备份VM

```
Output Rule:
	the primary VM may not send an output to the external world, until the backup VM has received and acknowledged the log entry associated with the operation prducing the output.（只是延迟的了输出，但并未停止VM的继续执行）
```

#### 错误检测

通过UDP心跳以及监控`logging channel`上的流量，判断服务器是否崩溃

但是在备份服务器没有收到心跳包时，有可能是主服务器崩溃，也有可能是网络错误。在网络错误时，主服务器实际还是在运行的，这时备份服务器的上线（`go live`）就会导致数据冲突（该问题称为`Split-Brain problem`）。

为了解决该问题，在上线之前，在共享存储上执行`test-and-set`操作，若执行失败，则自杀。若无法访问共享存储，则不断重试直到可以访问。



### Practical Implementation 

#### 启动重启FT VMs

`VMware VMotion`允许将正在运行的VM从一个服务器迁移到另外一个服务器。经过适当的修改，使得`FT VMotion`克隆VM到远程服务器而不是迁移，通过此就可以启动一个同主服务器状态相同的备份服务器。

当主服务器崩溃时，原先的备份服务器就会上线成为主服务器，而此时需要再启动一个新的备份服务器。`clustering service`通过资源利用率和其他限制条件决定在哪个服务器上建立备份VM。

#### 管理Logging Channel

虚拟机在`logging channel`两端维护一个缓冲队列，主服务器发送，备份服务器`log buffer`接受到后返回`ACK`。通过此可以使得`VMware FT`根据`Output Rule`知道什么时候发送被延迟的输出

当在主服务器的`log buffer`满时，它必须等待直到不满，这就会影响客户端。因此我们需要最小化主服务器`log buffer`满的可能性：

-   备份服务器处理速度太慢
-   使用额外机制降低主服务器的运行速度

#### Operation on FT VMs

多数操作需要通过`logging channel`保持主备的同步，比如主服务器关机，备份服务器也需要停止；主服务器资源管理变动，备份服务器也需要相应的变动

可以独立执行与主服务器或备份服务器的操作只有`VMotion`：

-   主服务器的VMotion：需要备份服务器切断与原主服务器的连接，建立与新主服务器的连接
-   备份服务器的VMotion：同上，还需要主服务器暂停IO

#### Disk IO实现细节

-   磁盘操作同时访问同一个磁盘位置或内存位置，会引起不确定性  **解决：检测竞争，使其按顺序执行**
-   磁盘操作会跟VM的应用对内存的操作冲突 **解决：1.MMU内存保护（代价昂贵） 2.bounce buffer( a temporary buffer that has the same size as the memory being accessed by a disk operation) 所有磁盘读写都对bounce buffer进行**
-   当备份服务器接替原主服务器，它并不确定之前的磁盘IO是否成功完成 **解决： 在备份服务器上线过程中，重新提交那些IO请求（即使它们已经执行完成）**

#### Network IO实现细节

-   关闭VM的 the asynchronous network optimizations，否则会引起不确定性，这就需要我们自己优化网络性能：
    -   implement clustering optimizations to reduce VM traps and interrupts
    -   reduce the delay for transmitted packets（**key：reduce the time required to send a log message to the backup and get an acknowledgement**）注册函数到TCP协议栈，使得当收到TCP数据时自动发送ACK
        -   quickly handle any incoming log messages on the backup and any acknowledgments received by the primary 
        -   when the primary VM enqueues a packet to be transmitted, we force an immediate log flush of the associated output log entry




### 替换性设计

-   使用非共享磁盘（距离较远时）：主备VM不使用共享磁盘，而是各自使用独立磁盘，在使用一套机制保证磁盘之间同步
-   在备份VM上执行磁盘操作：之前是主VM执行操作，将数据通过`logging channel`传送过去。可以不同传送数据，而是让备份VM执行相同的操作




## Raft(Week 3)

保证`replicated log`一致性的算法，相比与`Paxos`算法它更容易理解



### 算法

`Raft`将一致性问题分解为三个子问题：

-   领导选举（leader election）
-   日志复制（log replication）
-   安全问题（safety）

#### 基础概念

![](./doc/raft-server_state.png)

所有服务器只有三种状态：

-   `leader` ：管理着`replicated log`，它从`client`接受`log entries`并把这些`log entries`复制到其他服务器，同时告诉这些服务器什么时间可以安全的把这些`log entries`应用到他们的`state machine`
-   `follower` ：被动，只回应来自`leader`和`candidates`的请求，当`client`与`follower`联系，它直接重定位到`leader`
-   `candidate` ：`leader`的候选者



![](./doc/raft-term.png)

时间被分割成为无数个任意长度的`term`：

-   以连续的整数标号
-   每个`term`都是以`election`开头
-   在某些情况下，选举平票则此`term`没有选择出`leader`，很快就快进入一段新的`term`
-   每个服务器都会存储着一个`current term `，随着时间递增，在每一次与其他服务器通讯时都会交换`current term`进行比较
    -   如果一个服务器的比另一个服务器的小，则更新为较大值
    -   如果一个`leader`或者`candidate`发现其的值过期，则立马转变状态为`follower`
    -   如果一个服务器收到的请求有着过期的`term`，则拒绝请求

三种PRC