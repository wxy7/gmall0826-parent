package com.atguigu.gmall0826.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0826.common.constants.GmallConstant
import com.atguigu.gmall0826.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall0826.realtime.util.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * author : wuyan
  * create : 2020-02-15 14:31
  * desc : 
  */
object SaleApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    val orderInfoInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    //转换为样例类
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputDstream.map { record =>
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


    //转换为样例类
    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map { record =>
      val jsonString: String = record.value()
     val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }

    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(o => (o.id,o))
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(o => (o.order_id,o))

    //fullOuterJoin：保留了没有关联上的部分
    val orderFullJoinedDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[SaleDetail] = orderFullJoinedDstream.flatMap { case (orderId, (orderInfoOption, orderDetailOption)) =>
      val jedis: Jedis = RedisUtil.getJedisClient
      //一个orderInfo可能对应多个OrderDetail，存储join后的所有结果
      val saleDetailList = new ListBuffer[SaleDetail]


      if (orderInfoOption != None) {
        val orderInfo: OrderInfo = orderInfoOption.get
        if (orderDetailOption != None) {
          //1.1同一批次内，和orderdetail join
          val orderDetail: OrderDetail = orderDetailOption.get
          val saleDetail = new SaleDetail(orderInfo, orderDetail)
          saleDetailList += saleDetail
        }

        //1.2将自己写入redis
        //redis type : string   key: order_info:[orderid] value : orderinfo
        //为啥不用hash key=> 多个filed value
        //不需要一次取出整个清单
        //超大hash，不容易进行分布式存储
        //没法给filed-value设置单独的过期时间，只能设置key-value整个过期时间
        val orderInfoKey = "order_info:" + orderInfo.id
        //SerializeConfig解决：用fastjson时，将javabean转换不需要加，如果是scala的样例类对象，需要加
        val orderInfoJson: String = JSON.toJSONString(orderInfo,new SerializeConfig(true))
        jedis.setex(orderInfoKey, 600, orderInfoJson)

        //1.3从redis查对应的orderdetail
        val orderDetailKey = "order_detail:" + orderInfo.id
        val orderDetailsSet: util.Set[String] = jedis.smembers(orderDetailKey)
        if (orderDetailsSet.size() > 0 && orderDetailsSet != null) {
          import scala.collection.JavaConversions._
          for (orderDetailJson <- orderDetailsSet) {
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
        }
      } else {
        //orderinfo没有，orderdetail一定有
        val orderDetail: OrderDetail = orderDetailOption.get
        val orderDetailKey = "order_detail:" + orderDetail.order_id

        //2.1将自己写入redis，由于一个orderInfo可能对应多个OrderDetail，所以不能用string，用set
        //redis type : set   key: order_detail:[orderid]  value : orderdetailJsons
        val orderDetailString: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
        jedis.sadd(orderDetailKey, orderDetailString)
        jedis.expire(orderDetailKey,600)

        //2.2从redis查对应的orderinfo
        val orderInfoKey = "order_info:" + orderDetail.order_id
        val orderInfoJson: String = jedis.get(orderInfoKey)
        val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
        val saleDetail = new SaleDetail(orderInfo, orderDetail)
        saleDetailList += saleDetail
      }
      jedis.close()
      saleDetailList
    }

    //0 存量用户 写一个批处理程序 把数据库用户批量导入到redis中 这里省略
    //1 user_info 进入到redis中
    val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_USER_INFO, ssc)
    userInputDstream.foreachRDD{rdd =>
      val userRDD= rdd.map(_.value())
      userRDD.foreachPartition{ userItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
       for (userJsonString <- userItr) {
         //存redis， type: string key : user_info:[userid] value : userinfoJson
         //目的 通过user_id查询用户信息
         val userInfo: UserInfo = JSON.parseObject(userJsonString,classOf[UserInfo])
         val userKey = "user_info:" + userInfo.id
         jedis.set(userKey,userJsonString)
       }
        jedis.close()
      }
    }

    //将双流join后的结果再去查询redis中的userInfo表，得到带有userinfo的saleDetail流
    val saleDetailWithUserDstream: DStream[SaleDetail] = saleDetailDstream.mapPartitions { saleDetailItr =>
      //mapPartitions 性能优化
      val jedis: Jedis = RedisUtil.getJedisClient
      val saleDetailListWithUser = new ListBuffer[SaleDetail]

      for (saleDetail <- saleDetailItr) {
        val userKey = "user_info:" + saleDetail.user_id
        val userJson: String = jedis.get(userKey)

        val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetailListWithUser += saleDetail
      }
      jedis.close()
      saleDetailListWithUser.toIterator

    }

//    saleDetailWithUserDstream.print()

    //批量存入es
    saleDetailWithUserDstream.foreachRDD{rdd =>
      rdd.foreachPartition{saleDetailItr =>
        val saleDetailWithUserList: List[(String, SaleDetail)] = saleDetailItr.toList.map(saleDetail => (saleDetail.order_detail_id,saleDetail))

//        println(saleDetailWithUserList.mkString(","))

        MyEsUtil.insertBulk(GmallConstant.ES_ALERT_SALE,saleDetailWithUserList)
//        print("wuyan")
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
