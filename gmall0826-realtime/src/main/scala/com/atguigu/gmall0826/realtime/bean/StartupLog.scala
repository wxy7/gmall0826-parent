package com.atguigu.gmall0826.realtime.bean

/**
  * author : wuyan
  * create : 2020-02-07 15:32
  * desc : 
  */
case class StartupLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long
                     ) {

}

