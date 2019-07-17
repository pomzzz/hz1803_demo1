package com.spark

import com.SparkUtils.{ConnectPoolUtils, JedisConnectionPool}
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

  /**
    * 2.1.全国各省充值业务失败量分布 (存入mysql中)
    * 统计每小时各个省份的充值失败数据量
    */
  def Result03(lines: RDD[((String, String), List[Double])]): Unit ={
    lines.foreachPartition(t=>{
      // 获取到mysql的连接池
      val conn = ConnectPoolUtils.getConnections()
      t.foreach(x => {
        // insert into table(?,?,?) values(?,?,?) varchar,varchar,bigint
        val sql = "insert into ProHour(Pro,Hour,Counts) values('"+x._1._1+"','"+x._1._2+"',"+(x._2(0)-x._2(2))+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      // 关闭连接
      ConnectPoolUtils.resultConn(conn)
    })
  }

  /**
    * 3.充值订单省份 TOP10（存入MySQL）
    * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，
    * 只保留一位小数，存入MySQL中，进行前台页面显示。
    */
  def Result04(lines: RDD[(String, List[Double])]): Unit ={
    lines.foreachPartition(t=>{
      // 获取mysql的连接池
      val conn = ConnectPoolUtils.getConnections()
      t.foreach(x=>{
        // 对省份的成功率进行计算 （保留一个小数）
        val rate = (x._2(2)/x._2(0)).formatted("%.2f")
        val sql1 = "insert into ProRate(Pro,count,rate) values('"+x._1+"','"+x._2(0)+"',"+rate+")"
//        val sql2 = "create table prorate1 select pro,rate from prorate order by rate limit 10"
        val state = conn.createStatement()
        state.executeUpdate(sql1)
//        state.executeUpdate(sql2)
      })
      // 归还conn
      ConnectPoolUtils.resultConn(conn)
    })
  }

  /**
    * 4.实时充值情况分布（存入MySQL）
    * 实时统计每小时的充值笔数和充值金额。
    */
  def Result05(lines: RDD[(String, List[Double])]): Unit ={
    lines.foreachPartition(t=>{
      val conn = ConnectPoolUtils.getConnections()
      t.foreach(x=>{
        val sql = "insert into HourCountRate(hour,count,money) values('"+x._1+"',"+x._2(0)+","+x._2(1)+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      ConnectPoolUtils.resultConn(conn)
    })
  }

}
