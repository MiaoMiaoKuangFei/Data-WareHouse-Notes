# SparkCore笔记

## 日志

> 2020.4.30 完成RDD的创建部分
>
> 2020.5.17 完成RDD的算子部分
>
> 2020.6.1 完成了Spark全部数据结构的学习

## 结构

> **<u>Spark三大数据结构</u>**
>
> RDD：分布式数据集
>
> 广播变量：分布式只读共享变量
>
> 累加器：分布式只写共享变量

# RDD

## RDD的基本理解

[Resilient Distribute Dataset 弹性分布式数据集]

### 理解

RDD是一个装饰性的概念，相当于每一层都给这个原来的RDD增加性地了一定的性质，比如每一次都将上一个RDD的结果传到了这一层的RDD手上，但是不光是传进来了之前的性质，而且每次对它的操作都相当于是一次增强。

### 概念

是`Spark`最基本的逻辑抽象，是一个**不可变**、**可分区**、里面的元素**可以并行**计算的集合。

对比`RDD`和`Java IO`：

- `RDD`从属于分布式的集群操作
- `Java IO`只能读具体的文件，但是不能同时读A机器和B机器的文件。
- `Java IO`先`new`出来，只有触发要读的时候才读，`RDD`一样，只有`collect`的时候才去读。
- 为什么要包装：一层一层的包装根本上是**封装逻辑**，也就是可以说RDD是将数据处理的逻辑进行封装。

比较JavaIO：

![image-20200517121219690](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517121219690.png)

![image-20200517121233912](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517121233912.png)

只有在最后readLine的时候才在读取数据，前面都相当于是一种装饰，是对功能的一种增强。

![image-20200517121334877](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517121334877.png)

collect是跟上面readLine一样，也是只有它是真正才读进去数据的。

#### 不可变

要理解这个不可变性，可以类比`String`是不可变字符串，`String`不是不能变，但是它所谓的成员方法都是给它`new`了一个新的`String`（比如`subString`），而不是在原本的字符串上操作的。那么RDD也有方法，但是不能改变它自己本身。如果中间数据被改了，那其他基于它的运算就不准确了。

`RDD`的执行是按照血缘关系延时计算的，也就是说计算不是立即执行的，而是等到需要用到它的时候才执行。血缘关系较长，可以通过持久化`RDD`来切断血缘关系。

![image-20200430170359921](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200430170359921.png)

（要想改变`RDD`中的数据，只能在现有`RDD`的基础上创建新的`RDD`）

由一个`RDD`转移到另一个`RDD`，必须用算子来实现。什么叫算子：解决问题时将问题的初始状态$\rightarrow$完成状态，中间是通过这个一系列的操作进行状态转换，这个操作就叫做算子（`Operator`）。`Spark`当中算子就是一个个方法。

有一些算子是转化计算逻辑的，这叫转换算子，`collect`这样的真正触发计算的，叫做行动算子。

#### 可分区

把数据进行分区，是方便把这个分区打包发给对应的`Executor`，保证并行。由代码决定到底代码要有多少个分区的。

![image-20200517095247448](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517095247448.png)

（这一部分是p20复习的时候讲的）移动数据不如移动计算，是发给`Executor`的原则。首选位置是一个Seq，不止一个，是一个本地化的概念：

进程本地化：最好的方式是数据和计算是在一个进程之中（进程本地化）。

节点本地化：不在同一个进程里，但是可以在同一个Node里

机架本地化。

#### 可并行

 多线程$=$假并行，真并发

真并行是多个任务真正可以同时进行。

不断地转换结构，达到了类似的功能。可以把`RDD`理解成一个共同具有的父类。

### 图解

![1](E:\2020年Spring课程笔记\实习申请\字节跳动\笔记\1.jpg)



## RDD的属性

1. 一组分区
2. 计算每个分区的函数
3. `RDD`之间的依赖关系（血缘），谁依赖谁，比如`RDD`要传入上一个`RDD`，这就是这两个`RDD`的依赖关系。（防止它丢失，一层一层存着）
4. 一个`Partitioner`，`RDD`的分片函数
5. 一个列表，存储存取`Partitioner`的优先位置，是一个列表

移动数据不如移动计算。

怎么判断这个任务给谁好，数据在`HDFS`上，如果数据节点和`executor`在同一个机器是最快的，`executor`是在`DataNode`上，所以任务要发给数据承载的`executor`，这叫移动计算，如果取另一个`executor`计算，再把数据移动回来，这就叫移动数据，移动数据明显比移动速度更慢，所以按照这个方式就可以排出一个优先关系。这就是这个列表存储的优先位置的概念。

`RDD`的缓存：

如果`RDD`特别长，防止这个`RDD`断了，可以把它的血缘关系缓存起来。

## RDD编程的模型

### 创建RDD的方法

- 集合中创建

- 外部存储中创建

- 从其他`RDD`创建（转换，`new`出来）

#### 集合中创建（从内存中创建）

可以使用下面两个函数：

- `parallelize`

- `makeRDD`

```scala
val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
val sc = new SparkContext(config);

//1. 从内存中创建makeRDD
//括号里的意思是传递Seq为一个有顺序的集合
val listRDD:RDD[Int] = sc.makeRDD(List(1,2,3,4));
listRDD.collect().foreach(println);

//2. 从内存中创建parallelize
val listRDD:RDD[Int] = sc.parallelize(List(1,2,3,4));

//上面两种实现是完全一样的，makeRDD是内部调用了parallelize的
listRDD.collect().foreach(println);

```



#### 外部存储中创建

```scala
//从外部存储中创建
//默认情况下，可以读取项目路径，也可以读取其他路径：HDFS
val value:RDD[String] = sc.textFile("in")//默认写法，默认逐行读取文件，都是字符串类型
//对比
val value:RDD[String] = sc.makeRDD(Array("1","2","3"))//这样读出来的也是字符串类型

```

为什么说是并行呢？

并行和分区有相关的关系，首先要理解一下分区的问题

```scala
//将RDD保存在文件中
liskRDD.saveAsTextFile("output")
```

执行之后，文件目录会多出来一个`output`文件夹，里面有两类文件，其中`crc`是文件是验证文件，其他文件是保存的文件，在演示中，有效的文件保存的是八个，`part`对应的是分区，对应的八个分区。为什么明明只有四个数据却给了八个分区？首先`defaultParallelism`是虚函数，具体实现是从配置参数中并行度获取回来，没有传入参数，`defaultValue`是用默认值，那么这个值是：

```scala
math.max(totalCoreCount.get(),2)
```

可用的`CPU`的内核总数和$2$之间取最大值，`local`的话`*`用的是当时本机的最大数量，所以本机是$8$那么并行度就是$8$，那么分区就是$8$，由于数据只有$4$个，所以必定会有空的。

如果用`fileRDD`那个对象（从外部存储中创建）来做，里面的内容就成了`minPartitions`就是最小分区数，这里是取小的。

```
math.min(totalCoreCount.get(),2)
```

读取文件时（文件的大小是不一样的），传递的分区参数为最小分区数，但不一定是这个分区数，取决于`hadoop`读取文件时分片规则。

举一个例子：

整个文件结构：

![image-20200517100147471](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517100147471.png)

假如文件里写的是123456，然后在`fileRDD`里设置为$2$

```scala
liskRDD.saveAsTextFile("output",2)
```

但是实际分区是$3$，`textFile`是`minPartitions`，也就是说最小可以使用的分区是$2$，但是不一定最后使用的就是这个数字，因为：`textFile`用的是`hadoopFile`，`hadoop`有切片的概念，`12345`分两个分区，那就是$\frac{5}{2}=2.5$个分区，除不尽，就只能用$3$个分区。

![image-20200517100235263](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517100235263.png)

意思是从第2个分区取2个字节，从第0个分区取2个字节，从第4个分区取1个字节。

有一些问题：

事实上part0里存了12345，part1和part2是空的：

计算分区和往分区里存数据的规则是不一样的，hadoop按行读取的，不是按照字符读取的，判断是不是`5>2`，如果是，就都存在第一个分区，如果输入：

```
1
2
3
4
5
```

结果是分别存了1，2，3；4，5；空。

按道理应该化成了每行一个，而且命令行里面，是这样输出的：

![image-20200517100654285](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517100654285.png)

原因是整个文件的字节是13，是661的分布

但是为什么是13个字节，因为事实上txt会存成：

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517100755424.png" alt="image-20200517100755424" style="zoom: 67%;" />



当前数据的切片是一行一行的，分区的数据和我们预想的是不一样的。

### RDD的转换

整体上分为Value类型和K-V类型

#### 单一Value类型

##### Map(func)类型

作用：返回一个新的RDD，这个RDD是由原来的func函数转换后组成

测试需求：创建一个1-10的数组的RDD，将所有的元素*2变成新的RDD

```scala
//map算子
val listRDD:RDD[Int]:sc.makeRDD(1 to 10)
val mapRDD:RDD[Int]: listRDD.map(x=>x*2)//产生了新的对象（装饰性）
//var mapRDD:RDD[Int]: listRDD.map(_*2)，语法糖，这样写也是可以的
mapRDD.collect().foreach(println)
```

计算在Executor中执行`_*2`会发给很多executor，这里map里的计算功能可能发给不同的executor，如果只有3个executor是334.

##### MapPartitions(func)类型

作用：类似于map，不同的是，它独立地在每一个分片（分区）中运行，因此在类型为T的RDD上运行的时候，func的类型必须是Iterator[T]=>Iterator[U]（什么意思，就是一个分区地T类型数据被拿到了，那么返回的是一个分区的U数据）。假设有N个元素，有M个分区，那么map的函数将被调用N此，mapPartitions将被调用M次，一个函数一次处理所有分区。



```scala
val listRDD:RDD[Int] = sc.makeRDD(1 to 10)//闭区间
//mapPartitions可以对RDD的所有分区进行遍历
val mapPartitionsRDD:RDD[Int] = listRDD.mapPartitions(datas=>{
	datas.map(_*2)//这里的map是scala的map，这不是RDD的那个map，这里的map不是返回RDD，是返回一个集合的，因为这里的数据类型为iterator
})
```

当这里的时候`datas.map(_*2)`算一个计算`_*2`不是一个计算，因为对于一个executor才算一个计算，所以发两次给executor即可，而之前需要10次，也就是需要10次网络交互，所以这种mapPartition最快。减少了发送到执行器执行的交互次数。而我们重点就是看网络交互的数量，内存中计算的速度其实是很快的，这一点不需要过多考虑。

整体发给executor，有可能executor内存没有那么大，如果分开发的话，分区并不会释放前面的，因为还有引用存在（JVM可达性分析，能找到你，就不会释放你，因为它会被找到，所以它不会被释放），所以有OOM的内存溢出。

将内存较大的时候建议使用这样的方法

##### mapPartitionsWithIndex(func)

关心的是每个分区号，之前是分别关心每个数据、每个分区。入口参数是`Int,Iterator[Int]=>Iterator[U]`

```scala
val listRDD:RDD[Int] = sc.makeRDD(1 to 10,2)//闭区间，把10个数放在2个分区
//mapPartitions可以对RDD的所有分区进行遍历
val indexRDD:RDD[(Int,String)]=listRDD.mapPartitionsWithIndex{
	case(num,datas)=>{
		datas.map((_,"分区号"+num))//括号里是参数，=>之后是逻辑
	}
}
```

结果：

![image-20200517103603739](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517103603739.png)

##### 图解以上三项

![image-20200517101630848](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517101630848.png)

![image-20200517103713429](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517103713429.png)

![image-20200517103747056](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517103747056.png)

分区是和任务相关的，任务和Executor相关

##### Spark Driver&Executor

算子当中的计算功能全都是由Executor来做的，Driver和Executor并不是一定一个机器，IO处理，var i一定能序列化，一定要能传过去才行。这是因为代码的执行位置不同。所以对象一定要一定要序列化。

![image-20200517163552974](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517163552974.png)

##### flapMap(func)

扁平化，类似于map，但是每个输入元素

```scala
val listRDD:RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))//相当于两个List各自是一个整体
val flatMapRDD:RDD[Int] = listRDD.flatMap(datas=>datas)//括号里能填上什么东西？答案是能迭代的东西都可以填进去
flatMapRDD.collect().foreach(println)
```

##### glom

功能：把每个分区的数字都映射到一个数组中

```scala
val listRDD:RDD[Int] = sc.makeRDD(1 to 16,4)//这里没有最小分区的概念，这里都是给4个分区，多余的数会给到最后一个分区里面
val glomRDD:RDD[Array[Int]]=listRDD.glom()//将一个分区的数据放到一个数组中
flatMapRDD.collect().foreach(println)
```

按照上面这个写法，打印出来不是我们想要的

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517110711536.png" alt="image-20200517110711536" style="zoom: 67%;" />

原因是什么，应该怎么解决？

打印的是array，最后一个打印应该是

```scala
 glomRDD.collect().foreach(array=>{//对于每个array，我要有中间的逻辑来处理
 	println(array.mkString(","))//用逗号作为连接字符来处理这个array
 })
```

这样就能打印出来

```
1,2,3,4
5,6,7,8
8,10,11,12
13,14,15,16
```

那现在换一下，假设前面的makeRDD写成：

```scala
val ListRDD:RDD[Int]=sc.makeRDD(1 to 8,3)
```

输出的就是

```
1,2
3,4,5
6,7,8
```

把一个分区的数据放在一个组里面。

作用是什么，比如求RDD里的最大值，但是一个List里的内容它有不同的分区，它不能直接去个max，只能分开求最大值，然后再对所有最大值求最大值，但是array就可以直接求最大值。

##### groupBy(func)

作用：按照传入函数的返回值进行分组，将相同的key的值放在一个迭代器

```scala
var listRDD: RDD[Int]=sc.makeRDD(1 to 4)
var groupByRDD:RDD[(Int,Iterable[Int])]=listRDD.groupBy(i->1%2)
```

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517113135108.png" alt="image-20200517113135108" style="zoom: 67%;" />

这个前面的数字就是括号内计算的结果，分组后形成了对偶元组（K-V），K表示分组的Key，V表示分组的数据集合（可迭代的意思）

##### filter

作用：按照条件对内容进行过滤

```scala
val listRDD: RDD[Int]=sc.makeRDD(1 to 4)
val filterRDD:RDD[(Int,Iterable[Int])]=listRDD.filter(i=>i%2==0)//偶数被抽取出来
```

运行结果为：

```
2
4
```

##### sample

作用：以指定的种子随机抽样出数量为fraction的数据，withReplacement标识是抽出的数据是否妨会，true是有放回的抽样，false是无放回的抽样，seed用于指定随机数生成的随机数种子。

放回：泊松，不放回：伯努利

![image-20200517113826347](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517113826347.png)

```scala
val listRDD: RDD[Int]=sc.makeRDD(1 to 4)
val sampleRDD:RDD[Int] = listRDD.sample(false,0.4,1)//0.4是一个概率阈值
```

同理，改成0.6，再改成0.4，观察一下结果

```
第一次(0.4,1)
2
3
4
10
第二次(0.6,1)
1
5
6
7
8
9
10
第三次(0.4,1)
2
3
4
10
```

第三次的结果和第一次结果一样，它并不是真正的随机数，而是一个伪随机数，因为随机算法是一定的。真正的随机数是拿时间戳当作种子`Random random=new Random(System.currTimeMillis());`。

从我们指定的数据集合中进行抽样处理，会根据不同的算法进行抽样（true或者false）true的话把赋分给高一些，有可能同样的数字会被重复选取。

热点倾斜，看哪个部分被抽取的数量多一些，有理由相信被抽取的数量多一些是因为它的数量的确是多的，这样来看抽样是有意义的

关于：

```javascript
math.random();//是static，公用一份内存
new Random().nextDouble();//由当前时间生成种子，是互相不一样的
```

##### distinct([numTasks])

作用：对原始的RDD进行去重之后形成新的RDD

```scala
val listRDD:RDD[Int]=sc.maekRDD(List(1,2,1,5,2,9,5,1))
val distinctRDD:RDD[Int] = listRDD.distinct()//顺序是被打乱了的
//distinctRDD.collect().foreach(println)
distinctRDD.saveAsTextFile("output")//确实有八个分区，输出结果和我们与预想的不太一样
```

说明Distinct的分区规则：

假设有四个分区，去重。

`List(1,2,1,5,2,9,5,1)`在distinct的话，分区没有改变，我们在说明的时候使用4个分区;

也不见得平均来分配，也没有一定的规律，数据被打乱重组了（shuffle），意味着一个RDD里的数据被打乱到其他的分区里面去了的操作，就叫做shuffle操作。只有把所有数据全做完了，才敢往下做。

![image-20200517120230045](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517120230045.png)

前面的操作是不会把顺序打乱的，分区没有交叉，所以计算0和1不需要等待2和3，而前面的打乱的顺序必须等待，所以Spark中所有转换算子中没有shuffle的算子速度比较快。

![image-20200517120429300](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517120429300.png)一个RDD里的数据被打乱到其他的分区里面去了的操作，就叫做shuffle操作，如果P0和P1合在一块，并不算shuffle。在shuffle情况下可以减少分区。

```scala
//使用distinct算子对数据去重，因为去重之后导致数据减少，所以可以改变默认的分区的数量
val distinctRDD:RDD[Int] = listRDD.distinct(2)//表示用2个分区来表示
```

在UI下可以看出：

![image-20200517163419226](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517163419226.png)

里面有读文件和写文件的过程，所以慢

##### coalesce(numPartitions)

作用：缩减分区数，用于大数据集过滤之后，提高小数据集的执行效率

```scala
val listRDD:RDD[Int]=sc.maekRDD(1 to 16)
println("缩减分区前="+listRDD.partitions.size)
val coalesceRDD:RDD[Int] = listRDD.coalease(3)//顺序是被打乱了的
println("缩减分区后="+listRDD.partitions.size)
distinctRDD.saveAsTextFile("output")
```

结果：

```
缩减分区前=4
缩减分区后=3
```

4个分区里面的数据是怎么变成3个的呢，分区规则是什么样的。所谓的缩减其实是合并分区，没有打乱重组，没有shuffle。

结果：

```scala
1,2,3,4
5,6,7,8
9,10,11,12,13,14,15,16
```

Spark Partition&Tasks

最基本划分的原则就是：一个分区一个任务（Task），一个任务会分配到Executor执行。

![image-20200517163901021](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517163901021.png)

是有先后执行的概念，map是2个任务，没有shuffle，两个RDD可以当作一个整体来做的，按数据流往下做就行，行云流水从左边的RDD的P0直接流入右边的RDD的P0，它们不用等待，分区数相同效率是最高的；但是distinct会暂停，左边要Write右边要Read，每个分区一个任务，整体是四个任务，是有先有后的。

##### repartition(numPartitions)

重分区，是成倍成倍的减少分区，如果只是简单的合并就会造成数据倾斜，那么避免数据倾斜就用一个重分区的方式。

![image-20200517164423707](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517164423707.png)

在可视化界面里是有shuffle的，glom可以把一个分区的放在一个array里面，所以这里可以看出奇数偶数分属不同分区。

repartition的算子，底层调用的就是coalesce的算子，但是不同的是传入的是true是必须是shuffle的。

##### sortBy

作用：排序

```scala
val rdd=sc.parallelize(1 to 16,4)
rdd.sortBy(x=>x)//注意这里不能写下划线，因为具体它是参数还是函数是ambiguous的，所以不能用语法糖。
```

参看源码：

![image-20200517164845607](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517164845607.png)

默认可以不传升序降序，但是默认是升序，`numPartitions`默认不传就是自身长度，**改变分区就是改变任务**。分区越多，任务越多，并行度越高，速度会越高。

#### 双Value操作

##### Union

作用：相当于对两个RDD之间取并集，**并不考虑去重**

![image-20200517165132368](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517165132368.png)

##### Subtract

作用：去除两个RDD中相同的元素，不同的RDD将会留下来，取差集

![image-20200517165236314](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517165236314.png)

##### Intersaction

作用：取交集

![image-20200517165321501](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517165321501.png)

##### cartesian

作用：笛卡尔乘积，注意笛卡尔乘积尽可能不要使用。

![image-20200517165412111](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517165412111.png)

##### zip

拉链，形成一对一对的RDD，默认这两个源RDD的partition数量和元素数量都是相同的。

![image-20200517165520261](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200517165520261.png)

scala中如果数量多怎么办呢，就无视多出来的这个元素就行，但是Spark里不行，必须保证数量一样，否则就报错。如果分区数多怎么办，Spark里仍然会报错。

#### Key-Value类型

##### partitionBy

对pairRDD自己来写分区器，如果原来的partitionRDD和现在对partitionRDD是一致的话就不进行分区，否则会生成shuffleRDD，也就是会形成shuffle过程。

![image-20200518103330637](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518103330637.png)

![image-20200518103117629](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518103117629.png)

传入分区数量，里面`getPartition`用的是模式匹配，所有没有key的都放在0号分区里，如果不是，那么拿一个hashCode和一个分区数量来进行计算新分区。

HBase Map为预算，这里是取模运算，但是这里不能按位与，因为这里不一定是$2^n$，所以还是取模运算。

```scala
//声明分区器
//继承Partitioner类
class MyPartitioner(partitions:Int) extends Partitioner{
	override def numPartitions:Int={
		partitions
	}
	override def getPartition(key:Any):Int={
		1
	}//写成花括号的形式是为了表现它是一个函数，而不仅仅是一个赋值
}
```

```scala
val listRDD = sc.makeRDD(List(("a",1),("b",2),("c",3)))

val partRDD:RDD[(String,Int)]=listRDD.partitionBy(new MyPartitioner(3))
```

结果是走进去所有的分区都给了$1$，那么其他$0$，$2$号分区都是空的，数据都存在了$1$分区。

##### groupByKey

作用：`groupByKey`把同一个`key`放一个分组里，其依据和`groupBy`的规则不同，`groupBy`的规则是按照自定义的规则分类，而这里是按照`Key`分类。

![image-20200518104503124](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518104503124.png)

##### reduceByKey

作用：返回一个`(K,V)`的`RDD`，使用指定的`reduce`函数将相同的`key`聚合在一起，`reduce`的任务的个数可以通过第二个可选的参数来设置。

![image-20200518105107924](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518105107924.png)

比较`reduceByKey`和`groupByKey`：

`reduceByKey`：按照`key`进行聚合，会有预聚合`combine`的操作，这个动作发生在`shuffle`之前。

如果`shuffle`过程中有预聚合的操作，性能可以得到提高。

![image-20200518105627961](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518105627961.png)

计算`RDD`中的求和，如果是要计算$(1,2,3,4)$的求和，但是它们可能不在同一个机器里。区别分区内和分区间，对分区里的数据先做聚合，再对不同的机器做聚合。分区内和分区间的聚合并不是的算法，那么这个时候需要引入`aggregateByKey`。这个算子可以在分区内和分区间定义不同的运算。

##### aggregateByKey

![image-20200518110257566](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518110257566.png)

分析：zeroValue不是零值，它只是代表着默认值。填入第一个参数为0代表着它要是没有与它配对的数值，那么这个时候就得设置一个默认值与它运算。只有**第一次运用**的时候（对每个分区第一个数操作之前先行加入这个默认值）才能用这个默认值。

可以理解为在对分区运算前添加了一个`KV`对，叫做`(key,<zeroValue>)`

`SeqOp`一般是一个分区内部会形成一个序列，是一个分区内的运算，`combOp`一般是分区之间才这么叫，所以一般是分区之间的运算规则。

需求：创建一个`pairRDD`，取出**每个分区**相同的`key`对应值的最大值，然后相加。分析需求，也就是分区内和分区间的运算规则不一样。

实现：

![image-20200518111334663](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518111334663.png)

![image-20200518111811598](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518111811598.png)

变体：要是把默认值都改成$10$，结果就是：

```scala
Array((b,10),(a,10),(c,10))
```

原因是，比如`a`，在第一个分区里，$10>3>2$所以选择了$10$，同理选择`b,c`。

##### foldByKey

通过对比`aggregateByKey`来了解这个`foldByKey`的作用，分区间和分区内是同样的函数

![image-20200518113910293](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518113910293.png)

这里体现不出`scala`的折叠的概念，不过可以把它当做是`aggregateByKey`的简化。

`foldByKey`：

![image-20200518114030405](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518114030405.png)

`aggregateByKey`：

![image-20200518114131062](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518114131062.png)

##### combineByKey

参数：要传入三个参数，每个参数都是一个函数，第一个参数不见得和分区内的参数一样，第一个参数相当于把V变成指定结构的样子，再进行内部运算和分区间运算。

需求：创建一个pairRDD，计算每一个KV对出现的次数的平均值

运用前面的技术，最多是把它的求和求出，但是没法记录次数后当做除数求平均值，所以这个时候需要转换一下结构。

分析最核心的代码：

```scala
val combine = input.combineByKey(
	(_,1),//转换类型为(_,1)
	(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),//分区内的计算规则，因为第一个值已经变成了元组，这里的v相当于是说的下一个用到的值，这个值叫做v，而不是它自己
	(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)
)
```

![image-20200518115320397](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518115320397.png)

图解底层原理：

![image-20200518142527389](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518142527389.png)

同一个功能用三种函数来实现：

- aggregateByKey

![image-20200518142744649](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518142744649.png)

- foldByKey

![image-20200518142758700](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518142758700.png)

- combineByKey

![image-20200518142817590](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518142817590.png)

![image-20200518142830003](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518142830003.png)

注意，`combineByKey`不能不写类型，因为它不能推断出来这个$C$是啥类型的，因为$C$和$V$类型并不相关。

但是前面的两种方法，可以通过$U$是什么类型的，但是加了$classTag$，在运行时反射，推断出类型。

##### sortByKey

作用：按照Key的数字进行排列，升序从小到大来排列，true时升序，false是降序

![image-20200518143342577](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518143342577.png)

注意一个问题，就是为什么分区是：

```
1，2
3，4，5
6，7，8
```

按理说不应该是2，2，4这样的吗。下面说明一下这个算法：

![image-20200518143641558](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518143641558.png)

发现分区就是左开右闭的$(0,2],(2,5],(5,8]$。

##### mapValues

map是对当前的数据进行转换，mapValues也做转换，但是不对K作转换，而是只对V做操作。

![image-20200518143930387](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518143930387.png)

##### join(otherDataset,[numTasks])

作用：两个数据表连接，首先联系一下MySQL里的join，这个作用就是两张表根据条件相连。那么在Spark里就是两个表的Key为连接点相互关联。

![image-20200518144751201](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518144751201.png)

如果在原来的tuple之上再进行join，它的作用就是：

![image-20200518144853540](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518144853540.png)

join的作用性能也比较低。

##### cogroup(otherDataset,[numTasks])

![image-20200518145015844](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518145015844.png)

每一个元素相当于自身形成了一个compactBuffer，可以看作每一个元素自身形成了一个集合。

![image-20200518145134236](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518145134236.png)

这两个的显著区别就是，当如果其中之一没有该key，那么不是不连接，而是连接起来之后保存为一个空值。

#### 实际案例

##### 数据结构与需求

![image-20200518150024271](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518150024271.png)

##### 分析

![image-20200518150008325](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518150008325.png)

#### 行动算子

转换算子看作是逻辑的封装，但是真正的执行逻辑的操作就是行动算子。

##### reduce(func)案例

通过func函数聚合RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。

![image-20200518151151439](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518151151439.png)

同样地，它也可以运用自定义的规则来聚合`K-V`对。

##### collect()

以数组的形式返回数据集的所有元素

##### count()

返回RDD中元素的个数

##### first()

返回RDD中的第一个元素

##### take(n)

返回RDD前$n$个元素

##### takeOrdered(n)

返回RDD排序后前$n$名的元素。

##### aggregate

这个和aggregateByKey的区别在于它不需要是(K,V)对。

![image-20200518151542155](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518151542155.png)

但是也有区别，比如把默认值改成`10`，那么这个结果是`Int=85`。在aggregateByKey里计算应应当是`75`。原因是aggregate是分区内是做加一个默认值的，但是分区间同样也要加默认值这样的一个操作的。

##### fold

分区内和分区间的算法是一样的时候，它相当于是`aggregate`的一种简化算法。

![image-20200518152002953](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518152002953.png)

结果和`aggregate`完全一致。

##### 三种保存的行动算子

> - saveAsSequenceFile()

> - saveAsTextFile()

> - saveAsObjectFile()

在保存文件的是时候对比这三种方法

1. `saveAsSequenceFile()`

每一个文件都是IntWritable，而且都是SEQ

![image-20200518152253524](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518152253524.png)

2. `saveAsObjectFile()`

![image-20200518152308572](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518152308572.png)

3. `saveAsTextFile()`

按照保存的规则，在每个对应文件里保存（保存规则在上面的笔记里记录了）

##### countByKey()

作用：针对`K-V`对的`RDD`，返回一个`(K,Int)`的`map`，那么表示每一个`key`对应的元素个数。

![image-20200518152510210](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200518152510210.png)

##### foreach()

作用：在数据集的每一个元素上，运用函数func进行更新。

区别：之前的`foreach`，是`rdd.collect.foreach`，那么`collect`已经行动完了，是`scala`中的`Array`作为`collect`的返回值，`Array`是存在`Driver`的内存里，所以这样执行的时候是在`Driver`的内存中运算的。

```scala
var rdd = sc.makeRDD(List(1,2,3,4))//Driver
var rdd1 = rdd.map(
	case i =>{
		i*2 //Executor
	}
)
/*-----------------------------------------------*/
var rdd = sc.makeRDD(List(1,2,3,4))//Driver
rdd.foreach(
	case i=>{
		print(i*2)//不返回，还是在Executor中运行
	}
)
/*-----------------------------------------------*/
var rdd = sc.makeRDD(List(1,2,3,4))//Driver
rdd.collect.foreach(
	case i=>{
		print(i*2)//Driver
	}//rdd.collect返回了一个数组，因为Array存在Driver的内存里
)
```

只要调用行动算子，就会如`collect()->runJob`，开始运行作业了，只要调用一次，就会产生一个作业。

### RDD中函数的传递

初始化是在Driver端中进行的，实际运行程序是在Executor中开始的，这样就涉及跨进程通信，是需要序列化的

#### 传递一个方法

`Object not serializable`错误，涉及到哪些是在Driver中，哪些是在Executor中。

![image-20200530214644187](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200530214644187.png)

![image-20200530214704209](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200530214704209.png)

算子里的逻辑是在Executor中，这是个成员方法，是来自于对象的方法，这个对象按道理也要传到Executor中，Search对象也在Driver中，但是Search也没法序列化，所以需要序列化：

```Scala
class Search(query:String)extends java.io.Serializable
```

这样就不会报错了

#### 传递一个属性

上面的代码和前面没什么区别，调用的是：

`getMatch2`，`query`是对象的属性，如果`Search`注释了序列化，那么就还是会报错，如果换一种方法，把它传给了一个局部对象，那么只用把这个局部对象（这个`q`对应的`query`）传进去，也就是把当前的变量变成了字符串。

### RDD依赖关系

$A\rightarrow B\rightarrow C$

$B$用到$A$，没有$A$就没有$B$，$C$用到$B$，没有$B$就没有$C$

$A,B,C$都是`RDD`，如果中间宕机了，那么$C$就不知道是从哪来的，所以需要有容错机制。所以这个依赖关系在每个`RDD`都存在。

`RDD`只支持粗粒度转换，也就是在大量的记录上执行的单个操作，将创建`RDD`的一系列`Lineage`记录下来，以便恢复丢失的分区。

如果RDD的部分分区数据丢失的时候，它可以根据这些血缘关系重新运算和恢复。

![image-20200531152633541](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531152633541.png)

保存了文件位置以及转换的规则。

RDD和它以来的父RDD的关系有两种不同的类型，也就是窄依赖和宽依赖。

#### 窄依赖

一个分区原封不动地给另一个分区，就叫窄依赖。也就是说一个父RDD的Partition最多被子RDD的一个Partition使用。

#### 宽依赖

其他的依赖就叫做宽依赖。（没有宽依赖这个类，所以就是依赖中没有是窄依赖就是宽依赖）。

多个子RDD的Partition会依赖于一个父RDD，会引起`shuffle`。

#### DAG

有向无环图

`maven`本身就不能有环，具体到执行必定有一个没有入度的结点。划分成不同的Stage。对于宽依赖，由于有Shuffle的存在，只能在parent RDD处理完成之后，才能进行下面的计算，所以**宽依赖是划分Stage的依据。**

![image-20200531154050092](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531154050092.png)

逻辑有点像是从后向前推 ，窄依赖就放在一个阶段当中，宽依赖就放在不同依赖当中，因为是需要等待的。**阶段和阶段之间是需要等待的。**

#### 任务划分

RDD任务分为：`Application`、`Job`、`Stage`、`Task`



`Job`：一个**行动算子**就会形成一个Job

`Stage`：根据RDD之间的依赖关系的不同（就是根据宽依赖规则），把不同的Job划分成不同的Stage。

举例：

![image-20200531155239145](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531155239145.png)

$$Stage=1+\sum\text{Shuffle}$$

同一个阶段中<u>有多少任务取决于最后`RDD`有多少个分区</u>。

比如这个图例，前面是$5$个任务，后面是$3$个任务，$5$个任务之间可以相互并行，而后面$3$个任务可以相互并行，但是前面3个和后面5个任务之间不可以相互并行。

注意：$\text{Application}\rightarrow\text{Job}\rightarrow\text{Stage}\rightarrow\text{Task}$都是$1\rightarrow n$的关系。

![image-20200531160229206](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531160229206.png)

源码这里的意思是找到Shuffle的依赖，每一个依赖都映射到一个Stage，这和刚刚所说的上面的公式是一致的。

 ![image-20200531160526149](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531160526149.png)

这一段的源码中，只要模式匹配的条件满足case中设定的条件，那么就会添加一个`shuffleDep`，这个里面就是一个`HashMap`。

划分好阶段之后，还需要提交，前面计算的每个分区都会对应一个`Task`。

![image-20200531161745992](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531161745992.png)

`Task`有了之后需要提交，当时的阶段是一个`TaskSet`，把多个任务和当前的阶段绑定在一起。

在`WordCount`代码中，对这个部分进行分析：

![image-20200531163231926](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531163231926.png)

看到`UI`里显示的就是显示着两个`Stage`，和之前的分析一致。

原来有$1$个阶段，而这个reduceByKey是有一个Shuffle，所以就会增加一个Stage。

![image-20200531164944254](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531164944254.png)

### RDD缓存

如果中间宕机了，那么容错的话就会从头开始，如果执行时间太长了，那么所以要把中间计算时间长或者比较重要的数据，要把数据以序列化的形式缓存在JVM的堆空间中。通过的方法叫做`persist`或者`cache`。

但不是这两个方法被调用的时候立即缓存，而是触发后面的action的时候，这个RDD会被混存在计算节点的内存中，以供重用。

存储级别有好多种：

![image-20200531165236678](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531165236678.png)

如果在存储级别的结尾加上`_2`就是把持久化数据存成两份

`OFF_HEAP`是堆外存。`JVM`的问题就是`GC`垃圾回收问题。这个`GC`并不能被准确地管理，那么就只能堆外向操作系统申请。

分区的好处和价值：

![image-20200531165529844](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531165529844.png)

![image-20200531165648795](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531165648795.png)

如果不操作`cache`，那么可能就会出现每一次时间戳都是增加的不一样的，但是现在时间戳都是一样的，是因为已经将时间戳缓存到`cache`中了。

![image-20200531165806479](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531165806479.png)

相当于在血缘关系中增加了一个缓存。缓存也是可能失效的，所以同时也要把原始的血缘记录下来，所以它虽然可以提高效率，但是不能完全保证它的可靠性。

### RDDCheckPoint

创建一个二进制的文件，把检查点的位置记录在检查点中。注意检查点一定要记录检查点的保存目录。所以首先需要`sc.setCheckPointDir("cp")`。

![image-20200531170208598](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531170208598.png)

<img src="C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531170247525.png" alt="image-20200531170247525" style="zoom:50%;" />

所以这个时候运行`.toDebugString()`，注意最后一定要有一个行动算子，比如课件里用的是`foreach`，如果不用，是在`.toDebugString()`里看不到的。因为这个数据保存在检查点是绝对安全的，是已经持久化的，所以其血缘就直接关联到`checkPoint`中。

### RDD分区器

`Spark`目前支持`Hash`分区和`Range`分区，用户也可以自定义分区，那么`Hash`分区时当前的默认分区。

而且，只有KV类型的RDD才有分区器，不是KV类型的RDD分区器的值是None，每个RDD的分区ID范围是`0-numPartitions-1`

Hash分区规则就是计算hashCode，并且除以分区的个数区域，如果余数$<0$，则用余数+分区的个数，最后返回的值就是这个key所在的`ID`。

Ranger分区：Hash分区可能会出现数据倾斜，那么RangePartitioner的作用是将一定范围内的数映射到某一个分区内，尽量保证每个分区内的数据量的均匀，而且分区和分区之间是有序的，方法是：

第一步，从整个RDD抽出样本数据，将样本数据排序，计算出每个分区最大的key值，形成一个Array[Key]类型的数组变量rangeBounds。（要求：数据间可以比较和排序，但是这样的话对数据有限制，限制比较多）

第二步，判断key在rangeBounds中所处的范围，给出key值在下一个RDD中的分区id下标，该分区器要求RDD的key类型必须是可排序的。

自定义分区器

继承org.apache.spark.Partitions实现下面三个方法：

>- `numPartitions`:Int：返回创建出来的分区数
>- `getPartition`：返回给定键的分区编号
>- `equals`判断相等性的标准方法

### 数据读取和保存

text、json、csv文件，还有一些系统的文件。

#### 文件类数据读取和保存

比较重要的是json这类文件，那么每一行就是一个json记录，所以要注意json文件中要把记录一行一行地存起来。

![image-20200531173812953](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531173812953.png)

#### 文件系统类数据读取与保存

##### MySQL连接

使用方法：

>1. 基本的信息
>2. 创建jdbcRdd的对象
>
>3. 按照参数列表，填入jdbcRDD的参数，主要是要获取数据库得连接对象，这个和Java中的操作是一样的（最终得到一个Connection）

###### 查询数据

参数列表：

![image-20200531174515575](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531174515575.png)

最后一个参数是将结果映射出来，每一行都映射一次。

![image-20200531174626505](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531174626505.png)

按照这个代码去运行，最后的结果就可以直接将里面的内容查询出来。注意要有上下限，这个范围是要把MySQL里的东西给不同的Executor，这里有一个切分的概念，否则数据放在一块效率太低了，所以必须要有`id`的范围条件。

###### 保存数据

把每一个tuple的数据放在数据库当中，使用foreach这样的行动算子，然后使用模式匹配，里面的用法和query过程完全一样。

![image-20200531174937145](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531174937145.png)

>1. 创建RDD
>2. 对于每个模式匹配成功的，进行Collection的获取
>3. 使用preparedStatement，然后进行保存

foreach是在Executor中执行，那么哪个Exector先执行是不知道的，所以顺序是不知道的，所以发出的集合第一个并不是数据库中保存的第一个。

在这里的实现并不是很优雅，因为连接对象是进程和进程直接的交互，性能明显不好，所以connection不能创建太多次，整个循环过程中只要实现一次就可以了。所以把Connection提出来，但是这样的话对象并没有序列化，具体而言是Connection是不可以序列化，那么没法从Driver发到Executor，Connection是无法序列化的，加不上序列化。

那么为了保证效率也保证不出错，所以使用`foreachPartition()`，所以对每个分区进行操作，直接对每个分区内的datas进行操作：

![image-20200531180153124](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200531180153124.png)

这样运行就不会报错了。原因在于：外面是在`Driver`上运行的，而`datas`的数据类型是集合，那么`foreach`就不涉及数据传递，都在`Executor`里进行的，所以每一个分区就是一个`Connection`，但是这也是有问题的，这是以分区为循环的，那么就很有可能出现`OOM`的问题。



##### HBase连接

首先要有一个配置对象：

```scala
val conf:Configuration=HBaseConfiguration.create()
```

然后要读取数据那张表的对象（是哪个表）：

```scala
conf.set(TabelInputFormat.INPUT_TABLE,"student")
```

然后再读取HBase读取数据：

```scala
val hbaseRDD:RDD[(ImmutableButesWritable,Result)]=sc.newAPIHadoopRDD(
	conf,
	classOf[TableInputFormat],
	classOf[ImmutableBytesWritable],
	classOf[Redult]
	)//主键，再加上查询结果
hbaseRDD,foreach{
    case(rowkey,result)=>{
        val cells:Array[Cell]=result.rawCells()
        for(cell<-cells){
            println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
    }
}
```

那么继续说明对写入数据的操作：

```scala
sc.makeRDD(List(("1002","zhangsan"),("1003","lisi"),("1004","wangwu")))

val putRDD::RDD[(ImmutableButesWritable,Result)]=RDDdataRDD.map{
    case (rowkey,name)=>{
        val put=new Put(Bytes.toBytes(rowKey))//每一行变成put
        put.addColumn(Bytes.toBytes("Info"),Bytes.toBytes("name"),Bytes.toBytes(name))//向其中添加行
        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)),put)
    }
}

val jobConf = new JobConf(conf)
jobConf.setOutputFormat(classOf[TableOutputFormat])
jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")//设定表

putRDD.saveAsHadoopDataset(jobConf)
```

# 累加器

```scala
def main(args:Array[String]):Unit = {
	val config:Sparkconf=new SparkConf().setMaster("local[*]").setAppName("WordCount")
	val sc=new SparkContext(config)
	val dataRDD:RDD[Int]=sc.makeRDD(Lits(1,2,3,4))
    
    val i:Int = dataRDD.reduce(_+_)//底层实现有点复杂，因为要把这个东西分别发给不同的Executor
	sc.stop()
}
```

改成：

```scala
var sum:Int=0
dataRDD.foreach(i=>sum=sum+i)
println("sum="+sum)//sum=0

```

结果并没有得到我们想要的结果，那么原因是什么：

`i=>sum=sum+i`在不同的Executor里面。`sum`是一个数字，它可以序列化，虽然网络通信不能直接传输数字，但是确实数字可以序列化。

下面是示意图：

`sum+i`相加可以在Executor实现，但是`sum+sum`没有可以执行的代码，而且没法把这两个数字传回`Driver`，所以最终结果只能是0。

![image-20200601104434646](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200601104434646.png)

使用累加器来共享变量，是只写的，那么读只能在内部读，所以我们更强调它写的效果，所以使用这个共享变量来累加数据。

```scala
//创建累加器变量
var accumulator:LongAccumulator = sc.longAccumulator
//dataRDD.foreach(i=>sum+i)
dataRDD.foreach{
	case i=>{
		//执行累加器的累加功能
		accumulator.add(i)
	}
}
```

累加器就是Spark把这个Executor的计算结果返回来。

## 自定义累加器

```scala
//声明累加器
//1. 继承父类AccumulatorV2
class WordAccumulator extends AccumulatorV2[jl.long,jl.long]{
//粗略的样子
}
//然后把参数列表改成[String,util.ArrayList[String]]
```

要理解其中的IN,OUT是什么，也就是输入和输出是什么，所以就是要有输入和返回，多个单词要返回成集合。

```scala
//2.实现抽象方法
//当前的累加器是否为初始状态
override def isZero:Boolean = {
	List.isEmpty
}

//向累加器中增加数据
override def add(v:String):Unit={
	if (v.contains("h")){
		list.add(v)
	}
}

//复制累加器对象
override def copy():AccumulatorV2[String,util.ArrayList[String]]-{
    new WordAccumulator()
}

//重置累加器对象
override def reset():Unit = {
    list.clear()
}

```

完成了重写之后，那么要创建累加器

```scala
val wordAccumulator=new WordAccumulator//光有这个不行，还要注册累加器
sc.register(wordAccumulator)
```

最后直接进行操作：

```scala
val dataRDD:RDD[String]=sc.makeRDD(List("hadoop","hive","hbase","Scala","Spark"),2)

dataRDD.foreach{
	case i=>{
		//执行累加器的累加功能
		wordAccumulator.add(i)
	}
```

最后输出结果，可以得到

```scala
println("sum="+accumulator.value)
```

的结果为：

> sum=["hadoop","hive","hbase"]

# 广播变量

使用广播变量减少数据的传输。它是一种调优策略

![image-20200601110744514](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200601110744514.png)

![image-20200601110729795](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200601110729795.png)

# Spark-Core总结

## 三大数据结构

1. RDD：

   - 定义

     - 数据集：存储的是数据的计算逻辑，只有触发行动算子的时候才能得到数据

     - 分布式：数据的来源，从不同的网络节点中获得；可以在不同的计算节点中计算；数据的存储也是分区的

     - 弹性：

       - 血缘（依赖关系）：不是一成不变的，比如检查点切断血缘关系，或者经过一些特殊的处理方案来简化依赖关系
       - 计算：Spark的计算基于内存的，性能特别高，但是也可以和磁盘灵活切换
       - 分区：Spark在默认分区后，可以通过指定的算子来改变分区数量
       - 容错：Spark在执行计算时，如果发生了错误，需要进行容错重试处理

     - Spark中数量：

       - Executor：默认是两个，但是是有弹性的，可以根据自己的规则去改变，可以通过提交应用的参数去进行设定

       - Partition：默认分为两种，读取文件采用的是Hadoop的切片规则，如果读取内存中的数据，可以根据特定的算法进行设定，可以通过其他算子进行改变

         举例：ReduceByKey

         ![image-20200601111926325](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200601111926325.png)

         红色的RDD就是self的部分，它的分区数和外面装饰之后的分区数是一致的。

         ![image-20200601112017656](C:\Users\lenovo\AppData\Roaming\Typora\typora-user-images\image-20200601112017656.png)

         多个阶段的场合，一个阶段的分区数是受到上一个阶段的分区数所决定的。但是可以在相应的算子中进行修改。

       - Stage：阶段是靠Shuffle来进行分Stage，$1(\text{resultStage}+\text{shuffle依赖的数量})$划分阶段的目的就是为了任务执行的等待，因为Shuffle的过程需要罗盘。

       - Task

         原则上一个分区就是一个任务，因为一个分区是有空间局部性的，一个任务会发给Executor，一个Executor能执行几个任务就是取决于Core的数量。但是实际任务应用中，可以动态调整。要考虑优先位置的问题，传输数据不如传输任务，交互数据是有可能导致交互的损耗。

   - 创建

     - 从内存中创建
     - 从存储中创建
     - 从其他RDD中创建（转换的概念）

   - 属性

     - 分区
     - 依赖关系
     - 分区器
     - 优先位置（第三方数据库读取数据，没有优先位置的概念）
     - 计算函数

   - 使用

     - 转换算子
       - 单Value类型
       - 双Value类型
       - K-V类型
     - 行动算子：$\text{run Job}$

2. 广播变量：分布式共享只读数据（优化策略）

3. 累加器：分布式共享只写数据