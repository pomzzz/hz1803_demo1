package com.Config

import java.io.InputStream
import java.util.Properties


object ConfigManager {

  private val prop = new Properties()
  //通过类加载器加载指定的配置文件
  try{
    val in_basic = ConfigManager.getClass.getClassLoader.getResourceAsStream("basic.properties")
    prop.load(in_basic)
  }catch{
    case e:Exception => e.printStackTrace()
  }
  def getProper(key:String):String={
    prop.getProperty(key)
  }
}
