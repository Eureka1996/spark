package com.wufuqiang.spark.project.spark

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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



//    val messages = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics) ;
    stream.map(_._2).count().print

    ssc.start()
    ssc.awaitTermination()

  }

}
