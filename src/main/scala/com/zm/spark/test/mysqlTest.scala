package com.zm.spark.test

import java.sql.PreparedStatement

import com.zm.spark.conf.ConfigurationManager
import com.zm.spark.constant.Constants
import com.zm.spark.utils.MysqlManager

/**
  * Created by Luolidong on 2017/11/17.
  */
object mysqlTest {
  def main(args: Array[String]): Unit = {
    val popMonitorSql = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_SQL)
    val popMonitorOneTableName = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HABSE_TABLE)
    val popMonitorOneTableFamily = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HBASE_TABLE_FAMILY)
    val popMonitorOneTableQualifier = ConfigurationManager.getProperty(Constants.TEST_POP_PROV_HBASE_TABLE_QUALIFIER)

    try {
      val conn = MysqlManager.getMysqlManager.getConnection
      var pstmt: PreparedStatement = null
      pstmt = conn.prepareStatement(popMonitorSql)
      var i = 0

      val lines =  List(("mininews#2017-11-12 00:01#2017-11-12",10),("tips#2017-11-12 00:01#2017-11-12",10))

      conn.setAutoCommit(false)
      lines.foreach(tuple=>{
        val dataSplit = tuple._1.split("#")
        if (dataSplit(0).compareTo("empty") != 0) {
          pstmt.setString(1, dataSplit(0))
          pstmt.setString(2, dataSplit(1))
          pstmt.setString(3,dataSplit(2))
          pstmt.setInt(4, tuple._2)
          pstmt.setInt(5, tuple._2)

          pstmt.addBatch()

          i += 1
        }
      })

      if (i > 0) {
        pstmt.executeBatch()
        conn.commit()
      }

      conn.close()
    } catch {
      case e:Exception => {e.printStackTrace();}
    }
  }
}
