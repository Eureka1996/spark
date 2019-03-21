package com.wufuqiang.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
  * @ author wufuqiang
  * @ date 2019/3/21/021 - 10:04
  **/
object DateUtils {
  val original_format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val target_format = FastDateFormat.getInstance("yyyyMMddHHmmss")
  val str = "2019-03-21 10:08:01"

  def getTime(time:String) ={
    original_format.parse(time).getTime
  }

  def parseToMinute(time:String) ={
    target_format.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute(str))
  }
}
