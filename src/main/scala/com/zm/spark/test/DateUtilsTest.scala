package com.zm.spark.test

import java.util.regex.Pattern
import java.util.{Locale, TimeZone}

import com.zm.spark.utils.DateUtils
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.time.FastDateFormat
import scala.io.Source
object DateUtilsTest {
  def main(args: Array[String]): Unit = {
//文件读取
val file=Source.fromFile("hhhh")
    for(line <- file.getLines)
    {
      println(line)
      println("---------------")
      val lineSplit = line.trim.split('\u0001')
      println(lineSplit.length)
      if (lineSplit.length >= 29) {
        println("uid " +lineSplit.apply(5))
        println("udi " +lineSplit.apply(21))
        println("url " +lineSplit.apply(29))
      }

//      lineSplit.foreach(println(_))
//              val uid = lineSplit(5)
//              val udi = lineSplit(21)
//              var url = lineSplit(29)

//      println("udi"+udi)
//      println("url"+url)
      println("---------------")
    }
    file.close


//    val aa = "aaaaaaaa"
//    val bb: Array[String] = aa.split('?')
//    println(bb.length)
//    bb.foreach(println(_))
//    println(bb(0))
  }
}
