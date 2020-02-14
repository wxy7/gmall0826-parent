package com.atguigu.gmall0826.realtime.bean

/**
  * author : wuyan
  * create : 2020-02-14 15:59
  * desc : 
  */
case class AlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)  {

}
