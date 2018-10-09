package com.zm.spark.example

import com.zm.spark.utils._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object exportHiveToRedis {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Useage: xxx.jar day database:table:rowkey:contents:[dynamic|fixation]:catId hbaseTable repartitionNum")
      return;
    }

    val day = args(0)
    val hiveTableConf = args(1)
    val hbaseTable = args(2)
    val repartitionNum = args(3).toInt

    val hiveTableConfSplit = hiveTableConf.split(':')
    val database = hiveTableConfSplit(0)
    val table = hiveTableConfSplit(1)
    val rowkey = hiveTableConfSplit(2)
    val contents = hiveTableConfSplit(3)
    val catIdConf = hiveTableConfSplit(4)
    val catId = hiveTableConfSplit(5)

    val curTime = System.currentTimeMillis().toString

    var isDynamic: Boolean = false

    if ("dynamic".equals(catIdConf)) {
      isDynamic = true
    } else if ("fixation".equals(catIdConf)) {
      isDynamic = false
    } else {
      println("need param [dynamic|fixation]:catId")
      return;
    }

    val sparkSession = SparkSession.builder()
      .appName("exportHiveToRedis" + ":database:" + database + ":table:" + table)
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sql("use " + database)
    val sql = "SELECT * FROM " + table + " WHERE day = '" + day + "'"
    println(database)
    println(sql)
    val hiveTableDF: Dataset[Row] = sparkSession.sql(sql).repartition(repartitionNum)

    hiveTableDF.foreachPartition(iter => {
      if (iter.hasNext) {
        val jedis = RedisClient.pool.getResource
        val pipleline = jedis.pipelined()
        var row: Row = null
        var rowkeyData: String = null
        var contentsData: String = null
        var catIdData: String = null
        var redisMapKey: String = null
        var redisMapField: String = null
        var redisTimeMapField: String = null

        while (iter.hasNext) {
          row = iter.next()
          rowkeyData = row.getAs[String](rowkey)
          contentsData = row.getAs[String](contents)

          if (isDynamic) {
            catIdData = "cat_" + row.getAs[String](catId)
          } else {
            catIdData = catId
          }

          if (rowkeyData != null && rowkeyData.length > 0
            && contentsData != null && contentsData.length > 0) {
            redisMapKey = "userData:" + rowkeyData
            redisMapField = hbaseTable + ":" + catIdData
            redisTimeMapField = redisMapField + ":updateTime"
            pipleline.hset(redisMapKey, redisMapField, contentsData)
            pipleline.hset(redisMapKey, redisTimeMapField, curTime)
            pipleline.expire(redisMapKey, 3600 * 24 * 30 * 3)    //3个月
          }
        }

        pipleline.sync()
        pipleline.close()
        RedisClient.pool.returnResource(jedis)
      }
    })
  }
}
