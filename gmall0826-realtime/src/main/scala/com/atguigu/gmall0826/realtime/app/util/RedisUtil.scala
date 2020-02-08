package com.atguigu.gmall0826.realtime.app.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * author : wuyan
  * create : 2020-02-08 9:32
  * desc : 
  */
object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {
    if(jedisPool==null){
      //      println("开辟一个连接池")
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    //    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //   println("获得一个连接")
    jedisPool.getResource
  }
}
