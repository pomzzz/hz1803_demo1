package com.SparkUtils

import java.text.SimpleDateFormat

/**
  *
  */
object Utils_time {
  // 时间工具类
  def counttime(starttime:String,endtime:String):Long={
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    //开始时间
    val stime = format.parse(starttime.substring(0,17)).getTime
    // 结束时间
    val etime = format.parse(endtime).getTime
    etime-stime
  }
}
