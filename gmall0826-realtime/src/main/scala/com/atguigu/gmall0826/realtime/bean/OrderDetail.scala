package com.atguigu.gmall0826.realtime.bean

/**
  * author : wuyan
  * create : 2020-02-15 14:50
  * desc : 
  */
case class OrderDetail(
                        id:String ,
                        order_id: String,
                        sku_name: String,
                        sku_id: String,
                        order_price: String,
                        img_url: String,
                        sku_num: String
                      ) {

}
