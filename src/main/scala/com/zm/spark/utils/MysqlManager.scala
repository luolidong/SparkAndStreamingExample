package com.zm.spark.utils

import java.sql.Connection


import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zm.spark.conf.ConfigurationManager
import com.zm.spark.constant.Constants

class MysqlPool extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  val jdbcDriver = ConfigurationManager.getProperty(Constants.MYSQL_JDBC_DRIVER)
  val user = ConfigurationManager.getProperty(Constants.MYSQL_USER)
  val password = ConfigurationManager.getProperty(Constants.MYSQL_PASSWORD)
  val url = ConfigurationManager.getProperty(Constants.MYSQL_URL)

  try {
    cpds.setJdbcUrl(url)
    cpds.setDriverClass(jdbcDriver)
    cpds.setUser(user)
    cpds.setPassword(password)
    cpds.setMaxPoolSize(200)
    cpds.setMinPoolSize(20)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def getConnection: Connection = {

    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }
}

object MysqlManager {
  var mysqlManager: MysqlPool = _

  def getMysqlManager: MysqlPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlPool
      }
    }
    mysqlManager
  }
}
