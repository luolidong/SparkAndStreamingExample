package com.zm.spark.test

import com.zm.spark.utils._
import redis.clients.jedis.Jedis


object SparkRedisTest {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = RedisClient.pool.getResource
    println(JedisUtil.jtype(jedis,"click:user_74541"))
    val zzset = JedisUtil.zrevrangeWithScores(jedis,"click:user_74541",0,-1)



  }
}
