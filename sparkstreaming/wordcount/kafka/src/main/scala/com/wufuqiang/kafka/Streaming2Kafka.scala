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

    val fromTopic = "from1"
    val toTopic = "to1"
    val brokers = "node1:9092,node2:9092,node3:9092"

//    val kafkaPro = Map[String,String](
//      "bootstrap.servers" -> "10.130.140.81:9092,10.130.141.68:9092,10.130.140.82:9092" ,
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.130.140.81:9092,10.130.141.68:9092,10.130.140.82:9092",
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
//      ConsumerConfig.GROUP_ID_CONFIG -> "kafka" ,
//      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
//
//    )
    val kafkaPro = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node1:9092,node2:9092,node3:9092" ,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka" ,
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
