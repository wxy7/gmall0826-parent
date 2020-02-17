package com.atguigu.gmall0826.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0826.canal.util.MyKafkaSender;
import com.atguigu.gmall0826.common.constants.GmallConstant;

import java.util.List;

/**
 * author : wuyan
 * create : 2020-02-11 16:00
 * desc :
 */
public class CanalHandler {
    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;//rowDataList是一个SQL单位执行的结果

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        if (this.rowDataList != null && this.rowDataList.size() > 0){
            //下单业务主表
            if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT){
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
            }else if(tableName.equals("order_detail") && eventType == CanalEntry.EventType.INSERT){
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
            }else if(tableName.equals("user_info") && (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE)){
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_USER_INFO);
            }
        }
    }

    public void sendKafka(List<CanalEntry.RowData> rowDataList,String topic){
        //下订单
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();//一行数据
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + ":" + column.getValue());
                //以行的形式存储在kafka,一行数据在kafka是一条消息
                jsonObject.put(column.getName(), column.getValue());
            }
            String jsonString = jsonObject.toJSONString();
            //发送延迟，双流join时如果不处理就会产生数据丢失
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            MyKafkaSender.send(topic, jsonString);
        }
    }

}
