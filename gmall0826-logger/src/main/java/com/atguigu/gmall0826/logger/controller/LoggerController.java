package com.atguigu.gmall0826.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall0826.common.constants.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * author : wuyan
 * create : 2020-02-06 14:41
 * desc :
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafkaTemplate;

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString) {
        System.out.println(logString);
        //加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        String logJsonString = jsonObject.toJSONString();

        //本地落盘成日志文件
        log.info(logJsonString);//log是一个对象

        //发送kafka
        if ("startup".equals(jsonObject.get("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, logJsonString);
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, logJsonString);
        }



        return "success";
    }
}
