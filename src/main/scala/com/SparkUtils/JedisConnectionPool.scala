package com.SparkUtils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  val config = new JedisPoolConfig
  // 最大连接
  config.setMaxTotal(30)
  // 最大空闲
  config.setMaxIdle(10)
  val pool = new JedisPool(config,"192.168.47.160",6379,10000,"123")
  // 获取Jedis对象
  def getConection():Jedis={
    pool.getResource
  }
}
