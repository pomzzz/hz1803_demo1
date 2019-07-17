package com.spark

import com.SparkUtils.JedisConnectionPool
import org.apache.spark.rdd.RDD


object JedisApp {

  // 1)统计全网的充值订单量, 充值金额, 充值成功数
  def Result01(lines:RDD[(String,List[Double])]){
    lines.foreachPartition(t=>{
      val jedis = JedisConnectionPool.getConection()
      t.foreach(x=>{
        // 充值订单数
        jedis.hincrBy(x._1,"count",x._2(0).toLong)
        // 充值金额
        jedis.hincrByFloat(x._1,"money",x._2(1))
        // 充值成功数
        jedis.hincrBy(x._1,"success",x._2(2).toLong)
        // 充值总时长
        jedis.hincrBy(x._1,"time",x._2(3).toLong)
      })
      jedis.close()
    })
  }

  // 2)实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
  def Result02(lines: RDD[(String, Double)]): Unit ={
    lines.foreachPartition(t=>{
      val jedis = JedisConnectionPool.getConection()
      t.foreach(x=>{
        // 每分钟 的订单量
        jedis.incrBy(x._1,x._2.toLong)
      })
      jedis.close()
    })
  }
}
