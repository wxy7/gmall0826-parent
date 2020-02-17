package com.atguigu.gmall0826.realtime.util


import java.util
//import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._

object MyEsUtil {
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (client != null) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }

  // 批量插入数据到ES
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {
    if (docList.size > 0) {
      val jest: JestClient = getClient
      //創建批量提交對象
      val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      //id  是es中的表的主键，即_id
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }

      val bulk: Bulk = bulkBuilder.build()
      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        items = jest.execute(bulkBuilder.build()).getItems

      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(jest)
        println("保存" + items.size() + "条数据")
        for (item <- items) {
          if (item.error != null && item.error.nonEmpty) {
            println(item.error)
            println(item.errorReason)
          }
        }

      }
    }
  }

  //第二種批量插入的寫法
  def insertBulk2(sourceList : List[(String,Any)],indexName : String) : Unit = {
    val jest: JestClient = getClient
    //創建bulkBuilder創建對象，這個對象build後，得到bulk
    //bulkBuilder可以緩存一批數據
    val bulkBuilder = new Bulk.Builder

    //將每一條數據存在bulk中
    for ((id,source) <- sourceList) {
      val index: Index = new Index.Builder(source).index(indexName).`type`("_doc").id(id).build()
      bulkBuilder.addAction(index)
    }

    //完成bulk的創建，裡面包含了很多條數據
    val bulk: Bulk = bulkBuilder.build()
    //執行批量插入並獲取插入結果
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
    println(s"保存了：${items.size()}條數據")
    jest.close()
  }


  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient

    val index: Index = new Index.Builder(Stud("zhang3", "zhang33")).index("gmall2019_stud").`type`("_doc").id("stu123").build()
    jest.execute(index)
    jest.close()
  }


  case class Stud(name: String, nickname: String) {

  }

}