package com.wufuqiang.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/12/012 - 10:02
  **/


object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //    如果使用了stateful的算子，必须要设置checkpoint
    //    在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint("./checkpoint/statefulwordcount")

    val lineDStream = ssc.socketTextStream("10-255-0-242",9999)

    val wordDStream = lineDStream.flatMap(_.split(" "))

    val word2CountDStream = wordDStream.map((_,1))

    //  val resultDStream = word2CountDStream.reduceByKey(_+_)

    val state = word2CountDStream.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}
