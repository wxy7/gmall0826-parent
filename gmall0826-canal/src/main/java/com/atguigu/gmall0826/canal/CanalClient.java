package com.atguigu.gmall0826.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * author : wuyan
 * create : 2020-02-11 15:30
 * desc :
 */
public class CanalClient {
    public static void main(String[] args) {
        //TODO 1 连接canal的server端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        while (true){
            canalConnector.connect();
            //TODO 2 抓取数据
            canalConnector.subscribe("*.*");//订阅所有库所有表
            //1个message对象包含了100个什么？
            //100个SQL单位 1个SQL单位 = 1个SQL执行后影响的row集合
            Message message = canalConnector.get(100);
            //1个entry = 1个SQL单位
            if (message.getEntries().size() ==0){
                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // TODO 3 把抓取到的数据展开 变成想要的格式
                    if (CanalEntry.EntryType.ROWDATA == entry.getEntryType()){
                        ByteString storeValue = entry.getStoreValue();//结果的压缩包
                        //rowChange是结构化或者反序列化的SQL单位
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //解压 一个SQL单位执行的结果
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //得到行集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //得到操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //得到表名
                        String tableName = entry.getHeader().getTableName();

                        // TODO 4 根据不同业务数据类型发送到不同的kafka主题
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }





    }
}
