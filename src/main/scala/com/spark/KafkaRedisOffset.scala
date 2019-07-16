package com.spark

import com.Constants.Constan
import java.lang

import com.SparkUtils.{JedisApp, JedisConnectionPool, JedisOffset}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constan.SPARK_APP_NAME_USER).setMaster(Constan.SPARK_LOCAL)
        //设置每秒钟每个分区拉取kafka的速率
        .set("spark.streaming.kafka.maxRatePerPartition","100")
        // 设置序列化机制
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(60))
    // 配置参数
    // 配置基本参数
    // 组名 topic
    val groupId = "test2"
    val topic = "socend"
    // 指定kafka的broker的地址（SparkStreaming程序消费过程中，需要和kafka的分区对应）
    val brokerList = "192.168.47.160:9092"
    // 编写kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个topic
    val topics: Set[String] = Set(topic)
    // 第一步获取offset
    // 第二步通过offset获取kafka数据
    // 第三步提交更新的offset
    var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)
    // 先判断下是否有数据
    val stream: InputDStream[ConsumerRecord[String, String]] = if(fromOffset.size==0){
      KafkaUtils.createDirectStream(ssc,
        // 本地策略
        // 将数据均匀的分配到各个Executor上面
        LocationStrategies.PreferConsistent,
        // 消费者策略
        // 可以动态增加分区
        ConsumerStrategies.Subscribe[String,String](topics,kafkas))
    }else{
      // 不是第一次消费
      KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset))
    }

    val value = ssc.sparkContext.textFile("E:\\大数据学习资料\\cdh\\项目（二）01\\充值平台实时统计分析\\cmcc.json")
      .map(t=>(t.split(" ")(0),t.split(" ")(1)))
    val broadcasts = ssc.sparkContext.broadcast(value.collect().toMap)

    stream.foreachRDD(rdd =>{
      val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseData = rdd.map(_.value()).map(t=>JSON.parseObject(t))
          // 过滤需要的数据（充值通知）
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(t=>{
            // 先判断一下充值结果是否成功
            val result = t.getString("bussinessRst") // 充值结果
            val money:Double = if(result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
            val feecount = if(result.equals("0000"))  1 else 0 // 充值成功数
            val starttime = t.getString("requestId") // 开始充值时间
            val stoptime = t.getString("receiveNotifyTime") // 结束充值时间
            (starttime.substring(0,8),List[Double](1,money,feecount))
          }).reduceByKey((list1,list2)=>{
          list1.zip(list2).map(t=>t._1+t._2)
    })
      // 处理第一个指标
      JedisApp.Result(baseData)
      // 将偏移量进行更新
      val jedis = JedisConnectionPool.getConection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
