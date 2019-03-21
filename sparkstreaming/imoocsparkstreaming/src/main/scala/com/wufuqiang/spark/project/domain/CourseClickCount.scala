package com.wufuqiang.spark.project.domain

/**
  * 课程点击数
  * @ param day_course 对应hbase中的rowkey
  * @ param click_count 对就的点击数
  */
case class CourseClickCount(day_course:String,click_count:Long)