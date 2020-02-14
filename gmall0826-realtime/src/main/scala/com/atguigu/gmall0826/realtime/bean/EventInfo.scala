package com.atguigu.gmall0826.realtime.bean

/**
  * author : wuyan
  * create : 2020-02-14 15:34
  * desc : 
  */
case class EventInfo(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,//启动还是事件
                     evid:String ,//事件行为id
                     pgid:String ,
                     npgid:String ,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long
                    )
