package com.wufuqiang.kafka

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * @ author wufuqiang
  * @ date 2019/2/18/018 - 14:55
  **/

//代理类
class KafkaProxy(broker:String){

  val prop = Map[String,Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,//用于初始化链接到集群的地址
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
  )

  val kafkaClient = new KafkaProducer[String,String](prop)


}


//代理类的工厂类
class KafkaProxyFactory(broker:String) extends BasePooledObjectFactory[KafkaProxy]{
  override def create(): KafkaProxy = new KafkaProxy(broker)

  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = new DefaultPooledObject[KafkaProxy](t)
}



object KafkaPool {
  private var kafkaPool:GenericObjectPool[KafkaProxy] = null
  def apply(broker:String):GenericObjectPool[KafkaProxy] = {
    if(kafkaPool == null){
      KafkaPool.synchronized{
        this.kafkaPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(broker))
      }

    }
    kafkaPool
  }

}
