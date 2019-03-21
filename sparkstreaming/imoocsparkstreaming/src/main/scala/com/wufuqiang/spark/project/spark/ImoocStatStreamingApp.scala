package com.wufuqiang.spark.project.spark

import com.wufuqiang.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.wufuqiang.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.wufuqiang.spark.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @ author wufuqiang
  * @ date 2019/3/20/020 - 21:51
  **/
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(60))

    val zkQuorum = "10-255-0-139:2181,10-255-0-197:2181,10-255-0-253:2181"
    val topics = "streamingtopic"


    val kafkaPro = Map[String,String](
      "bootstrap.servers" -> "10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092",//用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "wufuqiang" ,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    // 根据上一次的Offset来创建
    // 链接到了kafka
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPro, Set(topics))

    val logs = stream.map(_._2)
    //原始日志格式  65.23.134.142   2019-03-21 10:20:01     "GET /class/163.html HTTP/1.1"  500     http://cn.bing.com/search?q=Spark Streaming

    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var courseId = 0 ;
      if(url.startsWith("/class")){
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
      }
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))
    }).filter(_.courseId != 0)

//    cleanData.print()
    cleanData.map(x => {
      (x.time.substring(0,8) + "_" + x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })
        CourseClickCountDAO.save(list)
      })
    })

    cleanData.map(x=>{
      val search_name = x.referer.replaceAll("//","/").split("/")
      var host = ""
      if(search_name.size >= 2 )
        host = search_name(1)
      (x.time,host,x.courseId)
    }).filter(_._2 != "").map(x=>{

      (x._1.substring(0,8) +"_"+x._2+"_"+x._3,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords =>{
        val list = new ListBuffer[CourseSearchClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseSearchClickCount(pair._1,pair._2))
        })
        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
