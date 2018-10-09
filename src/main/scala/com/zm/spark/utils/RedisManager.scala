package com.zm.spark.utils

import com.zm.spark.conf.ConfigurationManager
import com.zm.spark.constant.Constants
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

class RedisPool extends Serializable {

  lazy val jedisPool: JedisPool = initPool()
  val host = ConfigurationManager.getProperty(Constants.REDIS_HOST)
  val port = ConfigurationManager.getProperty(Constants.REDIS_PORT).toInt
  val passwd = ConfigurationManager.getProperty(Constants.REDIS_PASSWORD)
  val timeout = ConfigurationManager.getProperty(Constants.REDIS_TIMEOUT).toInt
  val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxTotal(500)
  poolConfig.setMaxIdle(20)
  poolConfig.setMaxWaitMillis(1500)
  poolConfig.setTestOnBorrow(false)
  poolConfig.setTestWhileIdle(true)
  poolConfig.setBlockWhenExhausted(false)

  def initPool(): JedisPool = {
        if (passwd != null) {
          new JedisPool(poolConfig, host, port, timeout, passwd)
        } else {
          new JedisPool(poolConfig, host, port, timeout)
        }
  }

  def destoryJedisPool(): Unit = {
      jedisPool.destroy()
  }
}

object RedisManager {
  var RedisManager: RedisPool = _
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      RedisManager.destoryJedisPool()
    }
  }

  sys.addShutdownHook(hook)

  def getRedisManager: RedisPool = {
    synchronized {
      if (RedisManager == null) {
        RedisManager = new RedisPool
      }
    }
    RedisManager
  }


}