package com.wufuqiang.advertise

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/2/18/018 - 19:27
  **/
object AdvertiseAnysis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("advertise").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic = "advertise"

    val kafkaParams = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node1:9092,node2:9092,node3:9092" ,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka" ,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))

    stream.foreachRDD{rdd =>
      rdd.foreachPartition{items =>
        for(item <- items){
          println(item)
        }
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
