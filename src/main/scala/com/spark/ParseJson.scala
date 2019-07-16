package com.spark

import com.Constants.Constan
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object ParseJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constan.SPARK_APP_NAME_USER).setMaster(Constan.SPARK_LOCAL)
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val line: DataFrame = sQLContext.read.json("E:\\大数据学习资料\\cdh\\项目（二）01\\充值平台实时统计分析\\cmcc.json")
    line.show()
  }
}
