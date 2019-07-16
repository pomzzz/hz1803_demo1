package com.SparkUtils


import org.apache.spark.rdd.RDD


object JedisApp {
  def Result(lines:RDD[(String,List[Double])]){
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
      })
    })
  }
}
