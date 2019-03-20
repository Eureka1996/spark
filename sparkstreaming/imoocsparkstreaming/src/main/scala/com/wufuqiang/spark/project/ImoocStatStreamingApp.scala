package com.wufuqiang.spark.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/20/020 - 21:51
  **/
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    


  }

}
