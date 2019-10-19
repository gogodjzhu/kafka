# Producer
用户直接通过`KafkaProducer`实例来发送数据，在它的内部采用 **生产者/消费者** 模式来发送数据

KafkaProducer的核心属性包括：

- `Metadata` 保存集群元数据，包括当前关心的topic/partition/broker相关信息都在这里保存
- `RecordAccumulator` 直译叫做*记录累加器，*本质是生产者/消费者模式中的消息队列。不同的地方在于：因为KafkaProducer通过单实例来发送
数据，此累加器实际上是针对每个TopicPartition维护的一些列队列的集合。
- `Sender` 实现了`Runnable`接口，作为一个单独线程在工作。它是生产者/消费者模式中的消费者。
- `TransactionManager` 事务管理器，通过它实现`Exactly-Once`语义。



## 消息生产者流程


## 消息消费者流程

在`KafkaProducer`的构造方法中就创建并启动了`Sender`线程，在while循环中不断判断是否可以发送/接收消息。

> **Sender的事件驱动**
> 在常规的生产者/消费者模型中，消费者总是盯着消息队列来控制自己的消费速率——队列有数据则发送，反之则空闲等待。而Sender采用了依赖nio的事
件驱动来控制消费，当nio事件（连接建立/断开/可读/可写...）来临，触发执行响应的操作，发送数据只是其中一种事件。

> **在构造函数中启动线程**
> TODO 据说很不好，为什么？

Sender的核心属性包括：

- `KafkaClient` 是Sender的关键对象，定义了对集群通信的关键方法。默认实现为`NetworkClient`。包括最关键的两个办法
	- `send(request:ClientRequest,now:long):void` 将请求添加到发送队列
	- `poll(timeout:long,now:long):List<ClientResponse>` 执行网络通信工作(读/写)，将结果返回
- `RecordAccumulator` 跟KafkaProducer共用一个实例. 
- `Metadata` 同上
- `TransactionManager` 同上

于是乎，我们很自然地勾勒出消息消费的流程：

1. `Sender`线程尝试获取nio事件
2. `Sender`获取到nio可写事件(`SelectionKey#isWritable()`)时，去`RecordAccumulator`找到有待发送消息并发送，保存这个发送记录以便
执行发送完成后的回调。
3. `Sender`获取到nio可读事件(`SelectionKey#isWritable()`)时，解析返回结果，去步骤2的发送记录中找到对应记录，成功/失败执行对应操作。

有这个蓝图，下面开始庖丁解牛，分析各个步骤的具体实现。

### 网络通信的封装（NIO的封装）

网络通信最主要的是`KafkaClient`接口，它的实现类为`NetworkClient`，它的核心属性为：

- `Selectable` 异步、多channel的网络IO接口，默认实现为`Selector`, 内部封装了原生的`java.nio.channels.Selector`和
`java.nio.channels.SocketChannel`，能够支持对多个节点（每个节点一个SocketChannel，通过Map组织）的通信。
- `InFlightRequests` 本质是一个map, 用以保存已经发送等待返回的inflight请求, 以brokerId为key

#### org.apache.kafka.common.network.Selector

KafkaProducer没有使用诸如netty,dubbo这样的通信框架，而是选择自己封装NIO，这就是`org.apache.kafka.common.network.Selector`, 它
的内部当然利用了NIO的`java.nio.channels.Selector`类，为了区分两者，参照代码，将后者称为`nioSelector`。

对nio的概念比较模糊的可以参考:[Java NIO极简说明](http://www.gogodjzhu.com/201906/java-nio-helloworld/)

在`Selector`内部包含以下重要属性：

- `nioSelector` nio原生接口，是Java NIO核心组件中的一个，用于检查一个或多个NIO Channel（通道）的状态是否处于可读、可写。如此可以实
现单线程管理多个channels,也就是可以管理多个网络链接。
- `channels:Map<String, KafkaChannel>` 维护的channel池，即连接池。key为Producer内部唯一的node标识；value是kafka自己封装的
`KafkaChannel`，它实现了NIO的channel接口可以替换channel工作，同时还定义了`receive:NetworkReceive`,`send:Send`属性以保存在当前
channel上发送/接收的数据。KafkaChannel提供了以下方法来编译/解析对此channel的请求/响应（当然最终还是调用了NIO Channel的`read`和
`write`方法）：
	- `setSend(send:Send):void` 将以当前`KafkaChannel`为目标的请求封装后set到`send`属性，因此这里同一时间只有一个`send`，因此显
	然上层通过单个`KafkaChannel`发送数据也是串行的。
	露了
	- `write():Send` 将Send封装的内容通过当前channel发送出去，此方法在`nioSelector`捕捉到此channel的`可写事件`后触发。Send的封装
	后文会介绍。
	- `read():NetworkReceive` 同上，当`nioSelector`捕捉到此channel的`可读事件`后，通过此方法将返回解析封装成`NetworkReceive`对象。

当然从Channel解析出来的`NetworkReceive`对象还只是将接收到的字节流封装成简单的`ByteBuffer`，单个channel接收到的所有`NetworkReceive`
被封装在一个队列中，由`Selector`中的`stagedReceives:Map<KafkaChannel, Deque<NetworkReceive>>`暂存。

*Selector处理的是最基础的网络传输，对于读操作来说，`KafkaClient`后续还需要对`stagedReceives`中的数据做拆包、解析；而对于写操作来说，
则需要在调用`setSend()`之前将请求合并/封装成Send对象（没错，Send本质上也是各种字节buffer）。具体怎么做，先挖个坑，本文不做深入。*

### 消息队列

> why RecordAccumulator ? 除了消峰填谷之外
> `KafkaProducer`的设计是线程安全的，可以并且也建议线程间共享同一个实例，因为生产中不同的线程通常会往不同的Topic/Partition中写入数据。
而多个Partition可能落在同一个Broker内，如果可以将这些消息做一次合并，那么不同线程往同一个Broker内多个Partition发送的多条新消息，将被
合并发送，这无疑有利于增加网络利用率。因此原因，RecordAccumulator被组织成这样的结构：
>
> TopicPartition -> Queue<ProducerBatch>
>
> 即为每个Partition都维护一个队列，队列内再按照Batch的方式保存多条消息。

`RecordAccumulator`包含以下几个核心属性：

- `free:BufferPool` ByteBuffer池。kafka选择自己维护内存，这样可以更灵活地使用预分配/容量控制等功能，此类封装了相关的方法。
- `batchSize:int` 发送到RecordAccumulator的消息被组织成一个个的`ProducerBatch`，batchSize控制每个batch的大小
- `lingerMs:long` 控制队列空闲多久之后，即便缓存的数据没有达到发送的大小，也执行发送。
- `batches:ConcurrentMap<TopicPartition, Deque<ProducerBatch>>` 实际缓存数据的地方。
- `incomplete:IncompleteBatches` // TODO



`RecordAccumulator`作为消息队列， `append()`方法是接收生产者数据的入口，分析代码：

```
# RecordAccumulator.java

append(){
	//...
	synchronize(deque){ // 对当前Partition所属的队列加锁，以保证并发安全
		// buffer是已分配空间的ByteBuffer实例，MemoryRecordsBuilder是操作此buffer的工具类，内部封装了具体的消息格式
		MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
		// batch是对一次 已发送/待发送的消息包（主意是消息包，可能包含了多条消息） 的最高抽象，封装了该消息包的回调方法
		ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
		// 将新的消息(key/value)添加到消息包中, 返回消息Future
		FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, headers, callback, 
			time.milliseconds()));
		// 将新建的消息包添加到队尾
		dq.addLast(batch);
		incomplete.add(batch);
		// 返回Future给本条消息的生产者
		return future;
	}
	// ...
}
```

最终新增的消息以Batch为单位，被放在了`batches`中。来看看`drain()`方法，这是消费者从`RecordAccumulator`拉取数据的方法，它的定义是这
样的：

```
# RecordAccumulator.java

/**
 * 获取以nodes为目标的records, 必要时会合并发往同一个node不同partition的多个record
 * @param cluster 当前Producer拥有的集群元数据
 * @param nodes 目标nodes集合，如果在cluster没有其中元素的信息，会在发送前拉取
 * @param maxSize 以单Node为目标的最大拉取长度，比如maxSize=100，即可以从队列中拉若干个size总和小于100的batch
 * @param now 当前时间
 * @return A list of {@link ProducerBatch} for each node specified with total size less than the requested maxSize.
 */
public Map<Integer, List<ProducerBatch>> drain(Cluster cluster,
                                               Set<Node> nodes,
                                               int maxSize,
                                               long now)
```

先略过`drain`方法的细节，把注意力放在获取了指向目标node的batchs之后做了什么。跟踪`drain`方法的调用，来到`Sender#sendProducerData()`
方法（事实上这也是唯一的调用方），而这个方法正是被Sender线程无限循环调用的核心方法：

```
#Sender.java

@Override // implement from Runnable
void run(long now) {
	// 从accumulator中拉取数据(batch), 放入({@link org.apache.kafka.common.network.KafkaChannel#send})
	long pollTimeout = sendProducerData(now); // 此方法中调用了drain方法
	// 调用client的poll方法, 执行发送KafkaChannel#send中的消息
	client.poll(pollTimeout, now);
}
```

那么drain获取了指向node的Batch集合之后，显然就是将生产者线程写入到`batches`中的数据封装成请求(`ClientRequest`)添加到待发送队
列(`send(request:ClientRequest,now:long):void`)，在nio可发送时将该请求发送出去。

### 回调策略
消息发送出去之后，server端会返回响应，下面来看看Producer是怎么做的。

入口方法回到`NetworkClient#poll()`:

```
#NetworkClient.java

@Override // impelment from KafkaClient
public List<ClientResponse> poll(long timeout, long now) {

	//...

	try {
		// 判断是否有新事件进来并做对应操作，这里我们关注的是有可读事件。调用链为:
		// Selector#pollSelectionKeys() -> Selector#addToStagedReceives()
		this.selector.poll(Utils.min(timeout, metadataTimeout, requestTimeoutMs));
	} catch (IOException e) {
		log.error("Unexpected error during I/O", e);
	}

	// 有数据进来之后会缓存在stagedReceives属性中, 下面的方法对其进行解析
	long updatedNow = this.time.milliseconds();
	List<ClientResponse> responses = new ArrayList<>();
	// 处理发送结果
	handleCompletedSends(responses, updatedNow);
	// 处理读取结果, metadata的更新也在这里做
	handleCompletedReceives(responses, updatedNow);
	// 处理断开连接
	handleDisconnections(responses, updatedNow);
	// 处理连接建立
	handleConnections();
	// 处理初始化api版本
	handleInitiateApiVersionRequests(updatedNow);
	// 处理超时
	handleTimedOutRequests(responses, updatedNow);
	// 完成请求，回调
	completeResponses(responses);

	return responses;
}

// 处理已完成接收的completedReceives
private void handleCompletedReceives(List<ClientResponse> responses, long now) {
	  for (NetworkReceive receive : this.selector.completedReceives()) {
			// source是Broker的编号
			String source = receive.source();
			/** 
			 * 从此broker的inFlight队列中移除并返回已完成的请求，腾出空间给新的请求 
			 * 写入inFlight队列的位置在{@link NetworkClient#doSend(ClientRequest, boolean, long, AbstractRequest)}
			 */
			InFlightRequest req = inFlightRequests.completeNext(source);
			Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(receive.payload(), req.header,
				 throttleTimeSensor, now);
			if (log.isTraceEnabled()) {
				 log.trace("Completed receive from node {} for {} with correlation id {}, received {}", req.destination,
					  ApiKeys.forId(req.header.apiKey()), req.header.correlationId(), responseStruct);
			}
			AbstractResponse body = createResponse(responseStruct, req.header);
			if (req.isInternalRequest && body instanceof MetadataResponse)
				 // 更新元数据操作在这里
				 metadataUpdater.handleCompletedMetadataResponse(req.header, now, (MetadataResponse) body);
			else if (req.isInternalRequest && body instanceof ApiVersionsResponse)
				 // 更新node版本，此请求往往是首先发出的
				 handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) body);
			else
				 responses.add(req.completed(body, now));
	  }
}

#Selector.java

void pollSelectionKeys() {
	if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
		NetworkReceive networkReceive;
		// selector有可读事件时，通过channel获取读取进来的数据，并缓存在networkReceive中
		while ((networkReceive = channel.read()) != null)
			addToStagedReceives(channel, networkReceive);
}

// 用队列缓存从这个channel获取到的所有NetworkReceive
private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
		// stagedReceives是Selector的实例属性
		if (!stagedReceives.containsKey(channel))
			stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());

		Deque<NetworkReceive> deque = stagedReceives.get(channel);
		deque.add(receive);
	}
}

```
