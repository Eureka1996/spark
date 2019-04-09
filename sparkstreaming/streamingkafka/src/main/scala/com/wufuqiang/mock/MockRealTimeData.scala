package com.wufuqiang.mock

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.ArrayBuffer

/**
  * @ author wufuqiang
  * @ date 2019/2/18/018 - 16:41
  **/
object MockRealTimeData {

  def generateMockData():Array[String]={

    val array = ArrayBuffer[String]()
    val random = new Random()

    for(i <- 1 to 50){
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(9) + 1
      val city = province
      val adid = random.nextInt(20)
      val userid = random.nextInt(100)
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }

    array.toArray
  }


//  创建Kafka的生产者
  def createKafkaProducer(broker:String,topic:String):KafkaProducer[String,String] ={

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String,String](prop)

  }

  def main(args: Array[String]): Unit = {

    val brokers = "10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092"
//    val brokers = "10.251.254.56:9092"
    val topic = "advertise"
//    val topic = "test"
    val kafkaProducer = createKafkaProducer(brokers,topic)
//    println(topic)
    while(true){
      for(item <- generateMockData()){

        kafkaProducer.send(new ProducerRecord[String,String](topic,item))
        println(item)

      }
      Thread.sleep(5000)
    }
  }
}
