package cn.utils

import com.typesafe.config.{Config, ConfigFactory}

object AppParmas {
  // 解析application.conf的配置文件
  // 加载resource下面的配置文件，   Application.conf->application.json->application.properties
 private val config: Config = ConfigFactory.load()
}
