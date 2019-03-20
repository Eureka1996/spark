package com.wufuqiang.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * @ author wufuqiang
  * @ date 2019/3/2/002 - 9:25
  **/

case class Employee(name:String,salary:Long)
case class Aver(var sum:Long,var count:Int)

class AverageSalNew extends Aggregator[Employee,Aver,Double] {

//  初始化方法，初始化每一个分区中的共享变量
  override def zero: Aver = Aver(0L,0)

//  每一个分区中的每一条数据聚合的时候需要调用该方法
  override def reduce(b: Aver, a: Employee): Aver = {
    b.sum += a.salary
    b.count += 1
    b
  }

//  将每一个分区的输出合并形成最后的数据
  override def merge(b1: Aver, b2: Aver): Aver = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

//  给出计算结果
  override def finish(reduction: Aver): Double = {
    reduction.sum.toDouble/reduction.count
  }

//  主要用于对共享变量进行编码
  override def bufferEncoder: Encoder[Aver] = Encoders.product

//  主要用于将输出进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


object AverageSalNew{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //hdfs://10-255-0-242:9000/data/employees.json
    val df = spark.read.json("hdfs://10-255-0-242:9000/data/employees.json")

    val aver = new AverageSalNew().toColumn.name("average")

    df.select(aver).show()

    //    spark.sql("select wfqavg(salary) from employees").show()
    spark.stop()
  }
}