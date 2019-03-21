package com.wufuqiang.spark.project.domain

/**
  * @ author wufuqiang
  * @ date 2019/3/21/021 - 16:35
  **/
/**
  * 从搜索引擎过来的点击量
  * @param day_search_course
  * @param click_count
  */
case class CourseSearchClickCount(day_search_course:String,click_count:Long)