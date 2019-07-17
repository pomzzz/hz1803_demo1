package com.spark

import com.Constants.Constan
import java.lang

import com.SparkUtils.{JedisConnectionPool, JedisOffset, Utils_time}
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
    val ssc = new StreamingContext(conf,Seconds(6))
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

    /**
      * 获取项目的城市文件，定义成广播变量
      */
    val citys = ssc.sparkContext.textFile("E:\\大数据学习资料\\cdh\\项目（二）01\\充值平台实时统计分析\\city.txt")
      // 切割取值 RDD[(String, String)]
      .map(t=>(t.split(" ")(0),t.split(" ")(1)))
    val broadcasts = ssc.sparkContext.broadcast(citys.collect().toMap)

    stream.foreachRDD(rdd =>{
      val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseData= rdd.map(_.value()).map(t=>JSON.parseObject(t))
          // 过滤需要的数据（充值通知）
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(t=>{
            // 先判断一下充值结果是否成功，
            // 不成功时，就不需要统计
            val result = t.getString("bussinessRst") // 充值结果
            val money:Double = if(result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
            val feecount = if(result.equals("0000"))  1 else 0 // 充值成功数
            val starttime = t.getString("requestId") // 开始充值时间
            val stoptime = t.getString("receiveNotifyTime") // 结束充值时间
            val counttime = Utils_time.counttime(starttime,stoptime) //充值使用的时间（开始时间-结束时间）
            (starttime.substring(0,8), //每天
              starttime.substring(0,10), // 每小时
              starttime.substring(0,12), // 每分钟
              List[Double](1,money,feecount,counttime)) //充值的订单数，金额，成功数，充值时长
          })

      /**
        * 1)统计全网的充值订单量, 充值金额, 充值成功数
        */
        // 按照每天作为key
      val result1: RDD[(String, List[Double])] = baseData.map(t=>(t._1,t._4)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      })
      JedisApp.Result01(result1)

      /**
        * 2)实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
        */
        // 获取baseData中的每分钟的值作为key，和订单数（List中的第一个数）
      val result2: RDD[(String, Double)] = baseData.map(t=>(t._3,t._4.head)).reduceByKey(_+_)
//      JedisApp.Result02(result2)



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
