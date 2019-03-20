package com.wufuqiang.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/14/014 - 12:49
  **/
object ForeachRddApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ForeachRddApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5)) ;



  }
}
