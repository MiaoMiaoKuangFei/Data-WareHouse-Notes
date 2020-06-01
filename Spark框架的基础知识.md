# Spark框架的基础知识

## 原理图

hadoop发现了很多的问题，那么本身的团队研发了Yarn，原来的结构中HDFS是存储数据的，负责计算的是MapperReduce，其中MapperReduce的问题比较大。

### 早期版本的架构

![image-20200518155827353](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518155827353.png)

- NameNode：用来管理DataNode的位置。

- DataNode：用来存放数据。

- JobTracker：调度任务

- TaskTracker：执行任务

MR的缺点：

> MR是基于数据集的计算，所以面向数据的，所以只对数据进行操作，它的运算规则是从存储介质中获取数据，然后进行计算，最后将结果存储会介质中。面临的都是单节点的业务需求，主要应用于一次性的计算。现在的计算需求中会有迭代的概念，不适合于数据挖掘和机器学习的迭代运算和图形挖掘计算。

> MR是基于文件存储介质操作，性能慢。

> MR和hadoop紧密耦合在一起，没有对拓展开放，无法动态替换
>
> JobTracker性能慢：既要调度资源也要调度任务。

### 新版本的架构

改进：Yarn的变化

> 主要是对计算部分进行的改进

![image-20200518155848291](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518155848291.png)

- ResourseManager：管理服务器可供使用的资源

- ApplicationManager：管理应用

- Driver：驱动器，这个才是和Task直接相连的，为了解耦，不会让Task和资源直接耦合，所以计算框架是可插拔的。

- Container：把Task和NodeManager计算的部分降低耦合性。

### Spark

Spark的计算是基于内存的，是迭代性计算的，scala是面向函数的编程，它是基于scala语法开发的。

![image-20200518160609836](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518160609836.png)

- Master：计算资源调度

- Worker：管理任务

- Executor：执行器，执行任务

- ApplicationMaster：降低Driver和Master的耦合性

> Container的作用是替换计算计算框架，但是Spark就用自己的计算框架，所以Spark不需要类似于Container的东西。

> 存储用hadoop来存储，所以HDFS还是得使用，Spark不能独立使用，所以真正的结构是HDFS+Spark+Yarn，其中Yarn当作一个可插拔的转接功能，由Yarn和Spark共同构成计算框架。

Spark内嵌模块

![image-20200518160345757](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518160345757.png)

### Spark如何与Yarn框架连接

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518160932722.png" alt="image-20200518160932722" style="zoom:50%;" />

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518160853295.png" alt="image-20200518160853295" style="zoom:67%;" />

```
Spark<-->Yarn<-->HDFS
```

1. 由Spark提交任务到Yarn集群(Yarn示意图的左边的部分)
2. 不能直接做计算，所以想办法创建一个应用的管理AM，逐步靠近要计算的需求
3. 但是不知道有哪些资源可以用，所以申请资源
4. 申请完了资源，我得返回回去可用的资源
5. 知道了资源，也知道要做什么计算，现在就要开始计算了，我们就选择一个NM来进行计算，在里面创建一个Spark的Container和Executor。（这里有种搭混了的感觉，我的理解是对于Container来说，这是可插拔的，所以我可以使用一个外接的Spark的Executor）
6. 反向注册任务，需要让AM知道我有什么Executor
7. 最后分解任务，然后调度任务

### 官方文档对于相关概念的总结

|      Term       |                           Meaning                            |
| :-------------: | :----------------------------------------------------------: |
|   Application   | User program built on Spark. Consists of a *driver program* and *executors* on the cluster. (构建于 Spark 之上的应用程序. 包含驱动程序和运行在集群上的执行器) |
| Application jar | A jar containing the user's Spark application. In some cases users will want to create an "uber jar" containing their application along with its dependencies. The user's jar should never include Hadoop or Spark libraries, however, these will be added at runtime. |
| Driver program  | The process running the main() function of the application and creating the SparkContext |
| Cluster manager | An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN) |
|   Deploy mode   | Distinguishes where the driver process runs. In "cluster" mode, the framework launches the driver inside of the cluster. In "client" mode, the submitter launches the driver outside of the cluster. |
|   Worker node   |    Any node that can run application code in the cluster     |
|    Executor     | A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors. |
|      Task       |       A unit of work that will be sent to one executor       |
|       Job       | A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. `save`, `collect`); you'll see this term used in the driver's logs. |
|      Stage      | Each job gets divided into smaller sets of tasks called *stages* that depend on each other (similar to the map and reduce stages in MapReduce); you'll see this term used in the driver's logs. |

## 三种模式

> 注：从现在开始向下，下面的笔记参考[尚硅谷的教程文档](https://zhenchao125.gitbooks.io/bigdata_spark-project_atguigu/content/chapter1/21-local-mo-shi.html)

### Local模式

Local 模式就是指的只在一台计算机上来运行 Spark.

通常用于测试的目的来使用 Local 模式, 实际的生产环境中不会使用 Local 模式.

#### 解压 Spark 安装包

把安装包上传到`/opt/software/`下, 并解压到`/opt/module/`目录下

```bash
tar -zxvf spark-2.1.1-bin-hadoop2.7.tgz -C /opt/module
```

然后复制刚刚解压得到的目录, 并命名为`spark-local`:

```bash
cp -r spark-2.1.1-bin-hadoop2.7 spark-local
```

------

#### 案例一：直接运行官方求`PI`的案例

##### Shell中运行

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master local[2] \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

> 注意

- 如果你的`shell`是使用的`zsh`, 则需要把`local[2]`加上引号:`'local[2]'`

> 说明

- 使用`spark-submit`来发布应用程序.

- 语法:

  ```bash
  ./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
  ```

  - `--master` 指定 `master` 的地址，默认为`local`. 表示在本机运行.
  - `--class` 你的应用的启动类 (如 `org.apache.spark.examples.SparkPi`)
  - `--deploy-mode` 是否发布你的驱动到 `worker`节点(`cluster` 模式) 或者作为一个本地客户端 (`client` 模式) (`default: client`)
  - `--conf`: 任意的 Spark 配置属性， 格式`key=value`. 如果值包含空格，可以加引号`"key=value"`
  - `application-jar:` 打包好的应用 jar,包含依赖. 这个 URL 在集群中全局可见。 比如`hdfs:// 共享存储系统`， 如果是 `file:// path`， 那么所有的节点的path都包含同样的jar
  - `application-arguments:` 传给`main()`方法的参数
  - `--executor-memory` 1G 指定每个`executor`可用内存为1G
  - `--total-executor-cores` 6 指定所有`executor`使用的cpu核数为6个
  - `--executor-cores` 表示每个`executor`使用的 cpu 的核数

- 关于 Master URL 的说明

| Master URL          | Meaning                                                      |
| ------------------- | ------------------------------------------------------------ |
| `local`             | Run Spark locally with one worker thread (i.e. no parallelism at all). |
| `local[K]`          | Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine). |
| `local[*]`          | Run Spark locally with as many worker threads as logical cores on your machine. |
| `spark://HOST:PORT` | Connect to the given [Spark standalone cluster](http://spark.apache.org/docs/2.1.1/spark-standalone.html) master. The port must be whichever one your master is configured to use, which is 7077 by default. |
| `mesos://HOST:PORT` | Connect to the given [Mesos](http://spark.apache.org/docs/2.1.1/running-on-mesos.html) cluster. The port must be whichever one your is configured to use, which is 5050 by default. Or, for a Mesos cluster using ZooKeeper, use `mesos://zk://...`. To submit with `--deploy-mode cluster`, the HOST:PORT should be configured to connect to the [MesosClusterDispatcher](http://spark.apache.org/docs/2.1.1/running-on-mesos.html#cluster-mode). |
| `yarn`              | Connect to a [YARN ](http://spark.apache.org/docs/2.1.1/running-on-yarn.html)cluster in `client` or `cluster` mode depending on the value of `--deploy-mode`. The cluster location will be found based on the `HADOOP_CONF_DIR` or `YARN_CONF_DIR` variable. |

##### 结果展示

该算法是利用蒙特·卡罗算法求`PI` ![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548942649.png-atguiguText)

备注: 也可以使用`run-examples`来运行

```bash
bin/run-example SparkPi 100
```

------

#### 案例二：WordCount

Spark-shell 是 Spark 给我们提供的交互式命令窗口(类似于 Scala 的 REPL)

**本案例在 Spark-shell 中使用 Spark 来统计文件中各个单词的数量.**

##### 步骤1: 创建 2 个文本文件

```bash
mkdir input
cd input
touch 1.txt
touch 2.txt
```

分别在 1.txt 和 2.txt 内输入一些单词.

##### 步骤2: 打开 Spark-shell

```bash
bin/spark-shell
```

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548950345.png-atguiguText)

##### 步骤3: 查看进程和通过 web 查看应用程序运行情况

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548950550.png-atguiguText)

地址: [http://hadoop201:4040](http://hadoop201:4040/)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548950702.png-atguiguText)

##### 步骤4: 运行 `wordcount` 程序

```scala
sc.textFile("input/").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect
```

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548985384.png-atguiguText)

##### 步骤5: 登录`hadoop201:4040`查看程序运行

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548985533.png-atguiguText)

####  提交流程

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1555552747.png-atguiguText)

####  wordcount 数据流程分析

1. `textFile("input")：`读取本地文件`input`文件夹数据；
2. `flatMap(_.split(" "))：`压平操作，按照空格分割符将一行数据映射成一个个单词；
3. `map((_,1))：`对每一个元素操作，将单词映射为元组；
4. `reduceByKey(_+_)：`按照key将值进行聚合，相加；
5. `collect：`将数据收集到`Driver`端展示。

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548986604.png-atguiguText)

### Standalone模式

#### 配置 Standalone 模式

##### 步骤1: 复制 spark, 并命名为`spark-standalone`

```bash
cp -r spark-2.1.1-bin-hadoop2.7 spark-standalone
```

##### 步骤2: 进入配置文件目录`conf`, 配置`spark-evn.sh`

```bash
cd conf/
cp spark-env.sh.template spark-env.sh
```

在`spark-env.sh`文件中配置如下内容:

```java
SPARK_MASTER_HOST=hadoop201
SPARK_MASTER_PORT=7077 # 默认端口就是7077, 可以省略不配
```

##### 步骤3: 修改 slaves 文件, 添加 worker 节点

```bash
cp slaves.template slaves
```

在`slaves`文件中配置如下内容:

```java
hadoop201
hadoop202
hadoop203
```

##### 步骤4: 分发`spark-standalone`

##### 步骤5: 启动 Spark 集群

```bash
sbin/start-all.sh
```

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548993355.png-atguiguText)

> #### 可能碰到的问题

- 如果启动的时候报:`JAVA_HOME is not set`, 则在`sbin/spark-config.sh`中添加入`JAVA_HOME`变量即可. 不要忘记分发修改的文件 ![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1554167648.png-atguiguText) ![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1554167972.png-atguiguText)

##### 步骤6: 在网页中查看 Spark 集群情况

地址: [http://hadoop201:8080](http://hadoop201:8080/)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548993466.png-atguiguText)

### 使用 Standalone 模式运行计算 PI 的程序

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://hadoop201:7077 \
--executor-memory 1G \
--total-executor-cores 6 \
--executor-cores 2 \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

<img src="http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548994124.png-atguiguText" alt="img" style="zoom: 33%;" />

### 在 Standalone 模式下启动 Spark-shell

```bash
bin/spark-shell \
--master spark://hadoop201:7077
```

> 说明:

- `--master spark://hadoop201:7077`指定要连接的集群的`master`

<img src="http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548997146.png-atguiguText" alt="img" style="zoom:50%;" />

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1548997283.png-atguiguText)

> 执行`wordcount`程序

```scala
sc.textFile("input/").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
res4: Array[(String, Int)] = Array((are,2), (how,2), (hello,4), (atguigu,2), (world,2), (you,2))
```

> 注意

- 每个`worker`节点上要有相同的文件夹:`input/`, 否则会报文件不存在的异常

### 配置 Spark 任务历史服务器(为 Standalone 模式配置)

在 Spark-shell 没有退出之前, 我们是可以看到正在执行的任务的日志情况:[http://hadoop201:4040](http://hadoop201:4040/). 但是退出 Spark-shell 之后, 执行的所有任务记录全部丢失.

所以需要配置任务的历史服务器, 方便在任何需要的时候去查看日志.

1. 配置`spark-default.conf`文件, 开启 Log

```bash
cp spark-defaults.conf.template spark-defaults.conf
```

在`spark-defaults.conf`文件中, 添加如下内容:

```java
spark.master                     spark://hadoop201:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://hadoop201:9000/spark-job-log
```

注意:

hdfs://hadoop201:9000/spark-job-log 目录必须提前存在, 名字随意

2. 修改spark-env.sh文件，添加如下配置.

```java
export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=30 -Dspark.history.fs.logDirectory=hdfs://hadoop201:9000/spark-job-log"
```

3. 分发配置文件

4. 启动历史服务

需要先启动 HDFS

```bash
sbin/start-history-server.sh
```

<img src="http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549009985.png-atguiguText" alt="img" style="zoom:50%;" />

> ui 地址: [http://hadoop201:18080](http://hadoop201:18080/)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549010317.png-atguiguText)

5. 启动任务, 查看历史服务器

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master spark://hadoop201:7077 \
--executor-memory 1G \
--total-executor-cores 6 \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549011281.png-atguiguText)

由于 master 只有一个, 所以也有单点故障问题.

### HA 配置(为 Master 配置)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549012453.png)

> 可以启动多个 master, 先启动的处于 Active 状态, 其他的都处于 Standby 状态

1. 给 spark-env.sh 添加如下配置

```java
# 注释掉如下内容：
#SPARK_MASTER_HOST=hadoop201
#SPARK_MASTER_PORT=7077

# 添加上如下内容：
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=hadoop201:2181,hadoop202:2181,hadoop203:2181 -Dspark.deploy.zookeeper.dir=/spark"
```

2. 分发配置文件

3. 启动 Zookeeper

4. 在 hadoop201 启动全部节点

```bash
sbin/start-all.sh
```

会在当前节点启动一个 master

5. 在 hadoop202 启动一个 master

```bash
sbin/start-master.sh
```

6. 查看 master 的状态

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549013310.png-atguiguText)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549013333.png-atguiguText)

7. 杀死 hadoop201 的 master 进程

hadoop202 的 master 会自动切换成 Active

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549013451.png-atguiguText)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549015444.png-atguiguText)

## Yarn 模式

### Yarn 模式概述

Spark 客户端可以直接连接 Yarn，不需要额外构建Spark集群。

有 `yarn-client` 和 `yarn-cluster` 两种模式，主要区别在于：Driver 程序的运行节点不同。

- `yarn-client：`Driver程序运行在客户端，适用于交互、调试，希望立即看到app的输出
- `yarn-cluster：`Driver程序运行在由 RM（ResourceManager）启动的 AM（AplicationMaster）上, 适用于生产环境。

> 工作模式介绍:

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549016476.png-atguiguText)

------

### Yarn 模式配置

#### 步骤1: 修改 hadoop 配置文件 yarn-site.xml, 添加如下内容：

由于咱们的测试环境的虚拟机内存太少, 防止将来任务被意外杀死, 配置所以做如下配置.

```xml
<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>
<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>
```

修改后分发配置文件.

#### 步骤2: 复制 spark, 并命名为`spark-yarn`

```bash
cp -r spark-standalone spark-yarn
```

#### 步骤3: 修改`spark-evn.sh`文件

去掉 master 的 HA 配置, 日志服务的配置保留着.

并添加如下配置: 告诉 spark 客户端 yarn 相关配置

```java
YARN_CONF_DIR=/opt/module/hadoop-2.7.2/etc/hadoop
```

#### 步骤4: 执行一段程序

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode client \
./examples/jars/spark-examples_2.11-2.1.1.jar 100
```

[http://hadoop202:8088](http://hadoop202:8088/)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549024822.png-atguiguText)

------

### 日志服务

在前面的页面中点击 history 无法直接连接到 spark 的日志.

可以在`spark-default.conf`中添加如下配置达到上述目的

```java
spark.yarn.historyServer.address=hadoop201:18080
spark.history.ui.port=18080
```

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549025405.png-atguiguText)

![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1549025423.png-atguiguText)

> **可能碰到的问题:**

如果在 yarn 日志端无法查看到具体的日志, 则在`yarn-site.xml`中添加如下配置 ![img](http://lizhenchao.oss-cn-shenzhen.aliyuncs.com/1556012782.png-atguiguText)

```xml
<property>
    <name>yarn.log.server.url</name>
    <value>http://hadoop201:19888/jobhistory/logs</value>
</property>
```