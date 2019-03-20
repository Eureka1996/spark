package com.wufuqiang.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/15/015 - 16:25
  **/
object WordCount extends App {

  val sparkConf = new SparkConf().setAppName("SparkcoreWordcount").setMaster("local[*]")
  val sc = new SparkContext(sparkConf) ;

  val result = sc.textFile("hdfs://10-255-0-242:9000/data/README.md").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  result.saveAsTextFile("hdfs://10-255-0-242:9000/sparkcorewordcount")

  sc.stop()

}
