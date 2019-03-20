package com.wufuqiang.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/2/17/017 - 20:47
  **/
object StreamingWordCount extends App {

  val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")

  val ssc = new StreamingContext(sparkConf,Seconds(5))

  val lineDStream = ssc.socketTextStream("10-255-0-242",9999)

  val wordDStream = lineDStream.flatMap(_.split(" "))

  val word2CountDStream = wordDStream.map((_,1))

  val resultDStream = word2CountDStream.reduceByKey(_+_)

  resultDStream.print()

  ssc.start()
  ssc.awaitTermination()

}
