package com.atguigu.gmall0826.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0826.common.constants.GmallConstant
import com.atguigu.gmall0826.realtime.app.bean.OrderInfo
import com.atguigu.gmall0826.realtime.app.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
  * author : wuyan
  * create : 2020-02-12 9:29
  * desc : 
  */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkConf = new SparkConf().setMaster("local[*]").setAppName("order_app")
    val ssc = new StreamingContext(spark,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    //转换为样例类
    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //将创建时间分成两个字段
      val dateHourArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateHourArr(0)
      orderInfo.create_hour = dateHourArr(1).split(":")(0)
      //电话号码脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
      orderInfo.consignee_tel = telTuple._1 + "****" + telTuple._2.splitAt(4)._2
      //每个订单 增加一个字段 标识是否是该用户首次付费 is_first_order (0,1)
      // 该用户之前是否是消费用户 如果有个表记录 所有已存在的消费用户清单（redis mysql hbase）
      orderInfo

    }

    orderInfoDstream.foreachRDD{rdd =>
      rdd.saveToPhoenix("GMALL0826_ORDER_INFO",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(),Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
