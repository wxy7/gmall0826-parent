package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util._

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constants.GmallConstant
import com.atguigu.gmall0826.realtime.bean.{AlertInfo, EventInfo}
import com.atguigu.gmall0826.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

/**
  * author : wuyan
  * create : 2020-02-14 15:23
  * desc : 
  */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")


    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT,ssc)

//    inputDstream.map(_.value()).print()

    //转为样例类
    val eventInfoDstream: DStream[EventInfo] = inputDstream.map {
      record => {
        val jsonString: String = record.value()
        val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])

        val format = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHour: String = format.format(new Date(eventInfo.ts))
        val dateHourArr: Array[String] = dateHour.split(" ")
        eventInfo.logDate = dateHourArr(0)
        eventInfo.logHour = dateHourArr(1)
        eventInfo
      }
    }

    //5分钟的窗口
    val windowDstream: DStream[EventInfo] = eventInfoDstream.window(Seconds(300),Seconds(5))

    //先转换结构
    val eventInfoWithMidDstream: DStream[(String, EventInfo)] = windowDstream.map(e => (e.mid,e))

    //按mid分组
    val groupbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWithMidDstream.groupByKey()


    val ifAlertInfoDstream: DStream[(Boolean, AlertInfo)] = groupbyMidDstream.map {
      case (mid, eventInfoItr) => {
        val uidSet = new HashSet[String]()
        val itemsSet = new HashSet[String]()
        val eventList = new ArrayList[String]()
        var ifClickItem: Boolean = false
        Breaks.breakable(
          for (eventInfo <- eventInfoItr) {
            //这个设备所有的行为集合
            eventList.add(eventInfo.evid)
            //累加领券的uid的个数
            if (eventInfo.evid == "coupon") {
              uidSet.add(eventInfo.uid)
              itemsSet.add(eventInfo.itemid)
            }

            //一旦有clickItem行为，就停止循环
            if (eventInfo.evid == "clickItem") {
              ifClickItem = true
              Breaks.break()
            }
          })

        //true:三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品
        val ifAlert = uidSet.size() >= 3 && !ifClickItem

        (ifAlert, AlertInfo(mid, uidSet, itemsSet, eventList, System.currentTimeMillis()))
      }
    }

     val AlertInfoDstream: DStream[AlertInfo] = ifAlertInfoDstream.filter(_._1).map(_._2)

    AlertInfoDstream.print()
    
    
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}
