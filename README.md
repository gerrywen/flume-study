# flume-study
## demo
### 打包并将jar包放置flume下的bin
```shell script
mvn clean package -Dmaven.test.skip=true
```


### 自定义Source
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-mysource.conf --name a1 -Dflume.root.logger=INFO,console
```

### 自定义sink
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-mysink.conf --name a1 -Dflume.root.logger=INFO,console
```

### 自定义interceptor
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-myinterceptor.conf --name a1 -Dflume.root.logger=INFO,console
```

### 自定义handler
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-HTTPSourceXMLHandler.conf --name a1 -Dflume.root.logger=INFO,console
```


### Flume-自定义 Interceptor（拦截器）
- 使用 Flume 采集服务器本地日志，需要按照日志类型的不同，将不同种类的日志发往不同的分析系统。
- 在实际的开发中，一台服务器产生的日志类型可能有很多种，不同类型的日志可能需要发送到不同的分析系统。
- 此时会用到 Flume 拓扑结构中的 Multiplexing 结构，Multiplexing的原理是，根据 event 中 Header 的某个 key 的值，将不同的 event 发送到不同的 Channel中，所以我们需要自定义一个 Interceptor，为不同类型的 event 的 Header 中的 key 赋予 不同的值。
- 这里以端口数据模拟日志，以数字（单个）和字母（单个）模拟不同类型的日志，需要自定义 interceptor 区分数字和字母，将其分别发往不同的分析系统（Channel）。
##### 编写 flume 配置文件
flume2 和 flume3 需要先启动，flume1 需要连接 flume2 和 flume3，若先启动 flume1 会报连接不上（也可以无视错误日志，先启动）

- 1.flume3，运行avro监听
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-flume3.conf --name a3 -Dflume.root.logger=INFO,console
```
- 2.flume2，运行avro监听
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-flume2.conf --name a2 -Dflume.root.logger=INFO,console
```

- 3.flume1，执行命令控制台
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-flume1.conf --name a1 -Dflume.root.logger=INFO,console
```  



## http
#### 自定义HTTPSourceAuthTokenHandler
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-HTTPSourceAuthTokenHandler.conf --name a1 -Dflume.root.logger=INFO,console
```

```shell script
curl -X POST -d'[{"headers":{"h1":"v1","h2":"v2","auth":"ccb123456"},"body":"hello body auth token success"}]'  http://localhost:50000
```


#### 自定义HTTPSourceAuthTokenHandler 和 HTTPSink
```shell script
./bin/flume-ng agent --conf conf --conf-file conf/wenguoli/flume-study-httpsink.conf --name a1 -Dflume.root.logger=INFO,console
```

 - 参考：[Apache-Flume日志收集+自定义HTTP Sink处理 测试用例搭建](https://blog.csdn.net/kkillala/article/details/82155845)

