package com.zm.spark.example

import com.zm.spark.conf.ConfigurationManager
import com.zm.spark.constant.Constants
import com.zm.spark.utils.{HbaseUtil, KafkaManager}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by dell on 2018/10/9.
  */
object TestPopByProv {

  def main(args: Array[String]): Unit = {

    //获取传入参数，便于调试
    var record = "5"
    var interval = "10"
    if (args.length != 0) {
      record = args(0)
      interval = args(1)
    }

    // Create StreamingContext
    val conf = InitSparkConf(record)
    val ssc = new StreamingContext(conf, Seconds(interval.toInt))
    val sc = ssc.sparkContext

    // Create direct kafka stream with brokers and topics
    val kafkaParams = InitKafkaParams
    val km = new KafkaManager(kafkaParams)
    val topicsSet = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_KAFKA_TOPICS_SET).split(",").toSet

    kafkaParams.foreach(println(_))
    topicsSet.foreach(println(_))

    val dataInputDStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // process
    StartProcess(dataInputDStream,ssc,km)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  def StartProcess(dataInputDStream: InputDStream[(String,String)], ssc: StreamingContext,km: KafkaManager) = {
    val popProvSql = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_SQL)
    val popProvHbaseTableName = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HABSE_TABLE)
    val popProvHbaseTableFamily = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HBASE_TABLE_FAMILY)
    val popProvHbaseTableQualifier = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HBASE_TABLE_QUALIFIER)


    dataInputDStream.foreachRDD(rdd=>{
      rdd.flatMap(line => {
        val data = line._2
        Some(data)
      }).mapPartitions(iter=>{
        //获取hbase连接connection
        val connection = HbaseUtil.getHbaseConn
        val popProvHbaseTable = connection.getTable(TableName.valueOf(popProvHbaseTableName))
        var result = ListBuffer[(String,Int)]()
        //TODO


        result.iterator
      }).reduceByKey(_+_).foreachPartition(iter=>{
        //获取mysql连接connection
        //TODO
      })

      km.updateZKOffsets(rdd)
    })

//    dataInputDStream.flatMap(line=>{
//      val data = line._2
//      Some(data)
//    }).mapPartitions(partition=>{
//      var result = ListBuffer[(String,Int)]()
//      val filepath: String = SparkFiles.get("GeoLite2-City.mmdb")
//      val database = new File(filepath)
//
//      val reader = new DatabaseReader.Builder(database).build
//      while (partition.hasNext) {
//        val line = partition.next()
//        val lineSplit = line.split('\t')
//        if (lineSplit.length > 4
//          && lineSplit(0).compareTo("") != 0
//          && StringUtils.isNumeric(lineSplit(0))) {
//          val ip = lineSplit(1)
//          val ipAddress = InetAddress.getByName(ip)
//
//          val response = reader.city(ipAddress)
//          val city = response.getCity
//
//          result.append((city.getNames.get("zh-CN"),1))
//        }
//      }
//
//      reader.close()
//      result.iterator
//    }).reduceByKey(_+_).print()
//
//    dataInputDStream.foreachRDD(rdd=>{
//      rdd.flatMap(line => {
//        val data = line._2
//        Some(data)
//      }).mapPartitions(iter=>{
//        //获取hbase连接connection
//        val connection = HbaseUtil.getHbaseConn
//        val popProvHbaseTable = connection.getTable(TableName.valueOf(popProvHbaseTableName))
//        var result = ListBuffer[(String,Int)]()
//
//        //获取geoIp数据库
//        val filepath: String = SparkFiles.get("GeoLite2-City.mmdb")
//        val database = new File(filepath)
//        val reader = new DatabaseReader.Builder(database).build
//
//        var i = 0
//
//        try {
//          while(iter.hasNext){
//            val line = iter.next()
//            val lineSplit = line.split('\t')
//            val timeStr = lineSplit(0)
//            val ip = lineSplit(1)
//            val uid = lineSplit(3)
//            val softName = lineSplit(5)
//// fix me
//            val ipAddress = InetAddress.getByName(ip)
//            val response = reader.city(ipAddress)
//            val subdivision = response.getMostSpecificSubdivision
//            val prov_id = subdivision.getIsoCode
//            val prov_name = subdivision.getNames.get("zh-CN")
//
//            val dateStr = DateUtils.parseDate(timeStr)
//            if ((softName.compareTo("KuaiZip.exe") == 0) && (softName.compareTo("KZReport.exe") == 0)) {
//              val rowkey = uid + "#" + dateStr
//              if (HbaseUtil.isNotExistKey(popProvHbaseTable,rowkey)) {
//                HbaseUtil.addRow(popProvHbaseTable,rowkey,popProvHbaseTableFamily,Array(popProvHbaseTableQualifier),Array("1"))
//                result.append((dateStr + "#" + prov_id + "#" + prov_name +"#" + uid,1))
//              }
//            } else {
//              result.append(("empty",1))
//            }
//          }
//        } catch {
//          case e:Exception => e.printStackTrace()
//        }
//
//        popProvHbaseTable.close()
//        reader.close()
//        result.iterator
//      }).reduceByKey(_+_).foreachPartition(iter=>{
//        //获取mysql连接connection
//        if (iter.hasNext) {
//          val conn = MysqlManager.getMysqlManager.getConnection
//          val pstmt = conn.prepareStatement(popProvSql)
//          var i = 0
//
//          conn.setAutoCommit(false)
//
//          try {
//            while (iter.hasNext) {
//              val popMonitorData = iter.next()
//              if (popMonitorData._1.compareTo("empty") != 0) {
//                pstmt.setString(1, popMonitorData._1.split("#")(0))
//                pstmt.setString(2, popMonitorData._1.split("#")(1))
//                pstmt.setString(3, popMonitorData._1.split("#")(2))
//                pstmt.setInt(4, popMonitorData._2)
//                pstmt.setInt(5, popMonitorData._2)
//
//                pstmt.addBatch()
//                i = i + 1
//              }
//            }
//
//            if (i > 0) {
//              pstmt.executeBatch()
//              conn.commit()
//            }
//          } catch {
//            case e:Exception => {e.printStackTrace();}
//          } finally {
//            pstmt.close()
//            conn.close()
//          }
//        }
//      })
//
//      km.updateZKOffsets(rdd)
//    })
  }

//  test=mininewsxktt.show->mininews,\
//  tpop4-3-sign.show->tpop
//  def GetProdMap(): Map[String,String] = {
//    var prodMap:Map[String,String] = Map()
//    for(prodMapStr <- ConfigurationManager.getProperty(Constants.KUAIZIP_POP_PROD_MAP).split(',')) {
//      val prodMapSplit = prodMapStr.split("->")
//      prodMap += (prodMapSplit(0)->prodMapSplit(1))
//    }
//    prodMap
//  }

  /**
    * Init SparkConf
    */
  def InitSparkConf(record: String): SparkConf = {
    new SparkConf()
      .setAppName("TestPopByProv")
      .set("spark.streaming.kafka.maxRatePerPartition", record)
      .set("spark.streaming.stopGracefullyOnShutdown","true")
//      .setMaster("local[2]")
      .set("spark.streaming.concurrentJobs", "4")
  }

  /**
    * Init SparkConf
    */
  def InitKafkaParams = {
    Map[String, String](
      "metadata.broker.list" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST),
      "group.id" -> ConfigurationManager.getProperty(Constants.TEST_POP_PROV_KAFKA_GROUP_ID),
      "fetch.message.max.bytes" -> "20971520",
      "auto.offset.reset" -> ConfigurationManager.getProperty(Constants.TEST_POP_PROV_KAFKA_OFFSET_RESET)
    )
  }
}
