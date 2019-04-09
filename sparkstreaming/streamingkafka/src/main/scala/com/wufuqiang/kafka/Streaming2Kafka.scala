package com.wufuqiang.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

/**
  * @ author wufuqiang
  * @ date 2019/2/18/018 - 14:30
  **/
object Streaming2Kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val fromTopic = "test1"
    val toTopic = "test2"
    val brokers = "10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092"


    val kafkaPro = Map[String,String](
      "bootstrap.servers" -> "10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092",//用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "wufuqiang" ,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPro,Set(fromTopic))

    stream.map{case (key,value) => "wfq:"+value}.foreachRDD(rdd =>
      rdd.foreachPartition{ items =>
        val kafkaProxyPool = KafkaPool(brokers)
        val kafkaProxy = kafkaProxyPool.borrowObject()
        for(item <- items){
          kafkaProxy.kafkaClient.send(new ProducerRecord[String,String](toTopic,item))
        }
        kafkaProxyPool.returnObject(kafkaProxy)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}
