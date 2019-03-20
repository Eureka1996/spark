package com.wufuqiang.advertise


import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ author wufuqiang
  * @ date 2019/3/3/003 - 15:06
  **/

object AdvertiseAnysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("advertise").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic = "advertise"



    val topicDirs = new ZKGroupTopicDirs("advertise-analyse",topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

//    创建了一个zookeeper对象
    val zookeeper = "node2:2181,node3:2181,node4:2181"
    val zkCLient = new ZkClient(zookeeper)

    val children = zkCLient.countChildren(zkTopicPath)

    var adRealtimeLogDStream :InputDStream[(String,String)] = null


    val kafkaParams = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node1:9092,node2:9092,node3:9092" ,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer" ,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "advertise-analyse",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ->"largest" //"smallest"
    )

    if(children >0){
      println("从zookeeper中恢复")

      var fromOffsets:Map[TopicAndPartition,Long] = Map()

//      为了获得一个topic所有的partition的主分区Hostname
      val topicList = List(topic)

      val request = new TopicMetadataRequest(topicList,0)
      val getLeaderConsumer = new SimpleConsumer("node1",9092,10000,10000,"offsetLookup")

      val response = getLeaderConsumer.send(request)

      val topicMetadataOption = response.topicsMetadata.headOption

      val partitions = topicMetadataOption match{
        case Some(tm ) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId,pm.leader.get.host)).toMap
        case None => Map[Int,String]()
      }
      getLeaderConsumer.close()

      println("partitions info is:" + partitions)
      println("children info is:" + children)

      for(i <- 0 until children){
        val partitionOffset = zkCLient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println("保存的偏移位置是："+partitionOffset)
        val tp = TopicAndPartition(topic,i)
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
        val consumerMin = new SimpleConsumer(partitions(i),9092,10000,10000,"getMinOffset")
        val curOffset = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        consumerMin.close()
        var nextOffset = partitionOffset.toLong

        if(curOffset.length > 0 && nextOffset < curOffset.head){
          nextOffset = curOffset.head
        }
        println("修正后的偏移位置是："+nextOffset)
        fromOffsets += (tp -> nextOffset)


      }
      val messageHandler = (mmd:MessageAndMetadata[String,String]) => (mmd.topic,mmd.message())

      adRealtimeLogDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets,messageHandler)
    }else{
      println("直接创建")
      adRealtimeLogDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set(topic))
    }



    var offsetRanges = Array[OffsetRange]()

    val abc = adRealtimeLogDStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    abc.foreachRDD{rdd =>
      for(offset <- offsetRanges){
        println(offset)
      }

      rdd.foreachPartition{items =>
//        处理了业务
        for(item <- items){

        }

      }

//      创建一个zookeeper的目录
      val updateTopicDir = new ZKGroupTopicDirs("advertise-analyse",topic)
//      创建 一个zookeeper的客户端
      val updateZkClient = new ZkClient(zookeeper)

//      保存了所有partition的信息
      for(offset <- offsetRanges){
        val zkPath = s"${updateTopicDir.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
      }
      updateZkClient.close()
    }

/*    adRealtimeLogDStream.foreachRDD{rdd =>
      rdd.foreachPartition{items =>
        for(item <- items){
          println(item)
        }

      }

    }*/

    ssc.start()
    ssc.awaitTermination()
  }

}
