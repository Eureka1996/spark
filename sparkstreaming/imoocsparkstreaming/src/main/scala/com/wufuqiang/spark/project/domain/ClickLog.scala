package com.wufuqiang.spark.project.domain

/**
  * @ author wufuqiang
  * @ date 2019/3/21/021 - 10:42
  **/

/**
  * 清洗后的日志信息
  * @param ip 日志访问的ip地址
  * @param time 日志访问的时间
  * @param courseId 日志访问的实战课程id
  * @param statusCode 日志访问的状态码
  * @param referer 日志访问的来源
  */
case class ClickLog (ip:String,time:String,courseId:Int,statusCode:Int ,referer:String)