package com.wufuqiang.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * @ author wufuqiang
  * @ date 2019/2/18/018 - 10:20
  **/
class CustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

//  程序启动的时候调用
  override def onStart(): Unit = {
    val socket = new Socket(host,port)
    var inputText = ""
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8))
    inputText = reader.readLine()


    while(!isStopped() &&  inputText != null){
//      如果接收到了数据就保存
      store(inputText)
      inputText = reader.readLine()
    }

    restart("")
  }

//  程序停止的时候调用
  override def onStop(): Unit = {

  }
}

object CustomReceiver{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lineDStream = ssc.receiverStream(new CustomReceiver("10-255-0-242",9999))

    val wordDStream = lineDStream.flatMap(_.split(" "))

    val word2CountDStream = wordDStream.map((_,1))

    val resultDStream = word2CountDStream.reduceByKey(_+_)

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}