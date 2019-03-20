package com.wufuqiang.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/14/014 - 8:57
  **/
object FileWordCount {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[*]") ;

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val fileLines = ssc.textFileStream("hdfs://10-255-0-242:9000/data/sparkstreamdata")
    val result = fileLines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
