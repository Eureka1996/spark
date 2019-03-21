package com.wufuqiang.spark.project.dao

import com.wufuqiang.spark.project.domain.CourseClickCount
import com.wufuqiang.spark.project.hbase.HBaseUtil
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
    * 保存数据到HBase
    * @ param list CouseClickCount
    */
  def save(list:ListBuffer[CourseClickCount]):Unit={

    for(item <- list){
//      HBaseUtil.addRow(tableName,item.day_course,cf,qualifer,item.click_count.toString)
      HBaseUtil.incrementColumnValue(tableName,item.day_course,cf,qualifer,item.click_count)
    }

  }



  /**
    * 根据rowkey查询点击量
    * @ param day_course
    * @ return
    */
  def count(day_course:String):Long  ={

    val result:Result = HBaseUtil.getARowByRowKey(tableName,day_course,cf);

    if(result.size()<=0)
      return 0L

    val value = result.getValue(Bytes.toBytes(cf) , Bytes.toBytes(qualifer))

    if(value == null){
      0L
    }else{
      Bytes.toLong(value)
    }

  }


  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8",8L))
    list.append(CourseClickCount("20171111_9",18L))
    list.append(CourseClickCount("20171111_1",1800L))
//    save(list)
    while(true){

      println(count("20190321_145"))
      Thread.sleep(10*60*1000)
    }
  }
}
