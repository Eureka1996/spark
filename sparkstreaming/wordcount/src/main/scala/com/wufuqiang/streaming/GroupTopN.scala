package com.wufuqiang.streaming

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ author wufuqiang
  * @ date 2019/4/13/013 - 17:20
  **/
object GroupTopN {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GroupTopN").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val text = sc.textFile("./GroupTopN.txt")
    text.map(line => {
      val words = line.split(" ")
      (words(0),words(1).toInt)
    }).groupByKey().map{case (key,value)=>{

      (key,value.toList.sortWith(_>_).take(4))
    }}.collect().foreach(item=>{
      val key = item._1
      val value = item._2
      print(key+" ")
      value.foreach(v=>print(v+" "))
      println()
    })


    sc.stop()
  }

}
