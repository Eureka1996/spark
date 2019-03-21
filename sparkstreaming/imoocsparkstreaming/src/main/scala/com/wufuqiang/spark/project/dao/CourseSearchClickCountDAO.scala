package com.wufuqiang.spark.project.dao

import com.wufuqiang.spark.project.domain.{CourseClickCount, CourseSearchClickCount}
import com.wufuqiang.spark.project.hbase.HBaseUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
  * 从搜索引擎过来的点击量-数据访问层
  */
object CourseSearchClickCountDAO {

  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"


  /**
    * 保存数据到HBase
    * @ param list CouseSearchClickCount
    */
  def save(list:ListBuffer[CourseSearchClickCount]):Unit={

    for(item <- list){
//      HBaseUtil.addRow(tableName,item.day_course,cf,qualifer,item.click_count.toString)
      HBaseUtil.incrementColumnValue(tableName,item.day_search_course,cf,qualifer,item.click_count)
    }

  }



  /**
    * 根据rowkey查询点击量
    * @ param day_course
    * @ return
    */
  def count(day_search_course:String):Long  ={

    val result:Result = HBaseUtil.getARowByRowKey(tableName,day_search_course,cf);

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
    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20171111_18",8L))
    list.append(CourseSearchClickCount("20171111_19",18L))
    list.append(CourseSearchClickCount("20171111_11",30L))
    save(list)
    println(count("20171111_11"))

  }
}
