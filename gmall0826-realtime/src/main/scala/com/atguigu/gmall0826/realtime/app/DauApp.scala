package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util.Set
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0826.common.constants.GmallConstant
import com.atguigu.gmall0826.realtime.app.bean.StartupLog
import com.atguigu.gmall0826.realtime.app.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * author : wuyan
  * create : 2020-02-07 14:14
  * desc :
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("day_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val recordStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

//    recordStream.map(_.value()).print()
    //TODO 1.将每一条日志转换成样例类对象
    val startupLogDstream: DStream[StartupLog] = recordStream.map {
      record => {
        val jsonString: String = record.value()
        val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])

        val format = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHour: String = format.format(new Date(startupLog.ts))
        val dateHourArr: Array[String] = dateHour.split(" ")
        startupLog.logDate = dateHourArr(0)
        startupLog.logHour = dateHourArr(1)
        startupLog
      }
    }

    //TODO 2.去重，保留每个mid当日的第一条，其他的启动日志过滤掉
    // TODO 然后利用清单进行过滤筛选 把清单中已有的用户的新日志过滤掉
    //方案一：redis连接太多，弃用
    /*
    val filteredDstream: DStream[StartupLog] = startupLogDstream.filter { startupLog =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val dauKey = "dau:" + startupLog.logDate
      val flag = !jedis.sismember(dauKey, startupLog.mid)
      jedis.close()
      flag
    }
    */


    /*
    //方案二错误，弃用：优化 利用driver查询出整个完整清单，然后利用广播变量发送给各个excutor
    //各个excutor利用广播变量中的清单 检查自己的数据 是否需要过滤
    //在清单中的一律洗掉
    //1. 查 driver查redis 在driver中只执行一次，即只拉取一次redis的数据 弃用
    val jedis: Jedis = RedisUtil.getJedisClient
    val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val dauKey = "dau:" + today
    val midSet: java.util.Set[String] = jedis.smembers(dauKey)
    jedis.close()
    // 2. 发送broadcast
    val midBC: Broadcast[java.util.Set[String]] = ssc.sparkContext.broadcast(midSet)
    //3. 收broadcast 过滤
    val filteredDStream: DStream[StartupLog] = startupLogDstream.filter { startupLog =>
      val midSet: java.util.Set[String] = midBC.value
      !midSet.contains(startupLog.mid)
    }
    */

    // TODO 2.1 不同批次去重 根据redis中当日的清单过滤不同批次之间的已有的mid
    //方案三：解决driver中只拉取redis一次的问题，通过transform
    val filteredDstream: DStream[StartupLog] = startupLogDstream.transform { rdd =>
      //每一个rdd执行一次，在driver中执行，而广播变量正好是在driver中发出
      println("过滤前：" + rdd.count())
      val jedis: Jedis = RedisUtil.getJedisClient
      val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val dauKey = "dau:" + today
      val midSet:  Set[String] = jedis.smembers(dauKey)
      jedis.close()
      val midBC: Broadcast[java.util.Set[String]] = ssc.sparkContext.broadcast(midSet)
      val filterRDD: RDD[StartupLog] = rdd.filter {
        startupLog =>
          val midSet: Set[String] = midBC.value
          //flag=true 说明不包含startupLog.mid 要留下
          //flag=false 说明包含startupLog.mid
          val flag = !midSet.contains(startupLog.mid)
          flag
      }
      println("过滤后：" + filterRDD.count())
      filterRDD
    }


    //TODO 2.2 同一批次去重
    //分组去重：0、转换成kv结构分组 1、先按mid进行分组  2、组内排序  3、取top1
    val groupbyMidDstream: DStream[(String, Iterable[StartupLog])] = filteredDstream.map(startupLog => (startupLog.mid,startupLog)).groupByKey()
    val realFilteredDstream: DStream[StartupLog] = groupbyMidDstream.map {
      case (mid, startupLogItr) => {
        val startupLogList: List[StartupLog] = startupLogItr.toList.sortWith {
          (s1, s2) => {
            s1.ts < s2.ts //正序排列
          }
        }
        val top1startupLogList: List[StartupLog] = startupLogList.take(1)
        top1startupLogList(0)
      }
    }



    //TODO 3.利用redis保存当日访问过的用户清单
    realFilteredDstream.foreachRDD{rdd =>
      rdd.foreachPartition{itr => {
        //性能优化 foreachPartition 一个分区建立一个redis连接
        val jedis: Jedis = RedisUtil.getJedisClient
        for (startupLog <- itr) {
          println(startupLog)
          val dauKey = "dau:" + startupLog.logDate
          jedis.sadd(dauKey,startupLog.mid)
          jedis.expire(dauKey,60*60*24)
        }
        jedis.close()
      }
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
