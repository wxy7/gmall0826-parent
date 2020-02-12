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
            if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT){
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
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
            MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonString);
        }
    }

}
