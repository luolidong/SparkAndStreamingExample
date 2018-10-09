package com.zm.spark.utils

import com.zm.spark.conf.ConfigurationManager
import com.zm.spark.constant.Constants
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient {
  val host = ConfigurationManager.getProperty(Constants.REDIS_HOST)
  val port = ConfigurationManager.getProperty(Constants.REDIS_PORT).toInt
  val passwd = ConfigurationManager.getProperty(Constants.REDIS_PASSWORD)
  val timeout = ConfigurationManager.getProperty(Constants.REDIS_TIMEOUT).toInt
  lazy val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxTotal(500)
  poolConfig.setMaxIdle(20)
  poolConfig.setMaxWaitMillis(1500)
  poolConfig.setTestOnBorrow(false)
  poolConfig.setTestWhileIdle(true)
  poolConfig.setBlockWhenExhausted(false)

  lazy val pool = initPool()

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook)

  def initPool(): JedisPool = {
    if (passwd != null) {
      new JedisPool(poolConfig, host, port, timeout, passwd)
    } else {
      new JedisPool(poolConfig, host, port, timeout)
    }
  }

}