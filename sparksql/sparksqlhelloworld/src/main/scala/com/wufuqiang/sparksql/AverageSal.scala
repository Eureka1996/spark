package com.wufuqiang.sparksql


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * @ author wufuqiang
  * @ date 2019/3/1/001 - 9:57
  **/
class AverageSal extends UserDefinedAggregateFunction{

//输入数据
  override def inputSchema: StructType = StructType(StructField("salary",LongType)::Nil)

//  每 一个分区中的共享变量
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",IntegerType)::Nil)

//  表示UDAF的输出类型
  override def dataType: DataType = DoubleType


//  表示如果有相同的输入是否会存在相同的输出，如果是则true
  override def deterministic: Boolean = true

//  初始化每一个分区中的共享变量
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0

  }

//  每一个分区中的每一条数据聚合 的时候需要 调用该方法
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    buffer(0) = buffer.getAs[Long]("sum") + input.getAs[Long]("salary")
    buffer(1) = buffer.getAs[Int]("count") + 1
  }

//  将每一个分区中的数据进行合并，形成最后的数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

//  给出最终的计算结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble /buffer.getInt(1)
  }
}

object AverageSal{
  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
////hdfs://10-255-0-242:9000/data/employees.json
//    val df = spark.read.json("D:\\wufuqiangbd\\ideaProjects\\spark2\\sparksql\\sparksqlhelloworld\\src\\main\\resources\\employees.json")
//
//    df.createOrReplaceTempView("employees")
//    spark.udf.register("wfqavg",new AverageSal())
//    spark.sql("select wfqavg(salary) from employees").show()
//    spark.stop()
//声明配置
    val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    /*.setJars(List("C:\\Users\\Administrator\\Desktop\\spark2\\sparkcore\\sparkwordcount\\target\\spark-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    .setIfMissing("spark.driver.host", "192.168.56.1")*/


    //创建SparkContext
    val sc = new SparkContext(sparkConf)

    //业务逻辑

    val file = sc.textFile("hdfs://10-255-0-242:9000/data/groupbykey.txt")

    val words = file.flatMap(_.split(" "))

    val word2count = words.map((_,1))

    val result = word2count.reduceByKey(_+_)

    result.collect
//    result.saveAsTextFile("hdfs://10-255-0-242:9000/abc6")

    //sc.textFile("hdfs://master01:9000/README.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://master01:9000/abc5")

    //关闭Spark链接
    sc.stop()
  }
}
