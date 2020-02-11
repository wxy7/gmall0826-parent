package com.atguigu.gmall0826.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * author : wuyan
 * create : 2020-02-10 18:33
 * desc :
 */
@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date) {
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map> list = new ArrayList<>();
        HashMap dauTotalMap = new HashMap();
        dauTotalMap.put("id", "dau");
        dauTotalMap.put("name", "新增日活");
        dauTotalMap.put("value", dauTotal);
        list.add(dauTotalMap);

        HashMap dauMidMap = new HashMap();
        dauMidMap.put("id", "new_mid");
        dauMidMap.put("name", "新增设备");
        dauMidMap.put("value", 233);
        list.add(dauMidMap);

        String result = JSON.toJSONString(list);

        return result;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        Map<String, Long> todayHour = publisherService.getDauTotalHour(date);
        Map<String, Long> yesterdayHour = publisherService.getDauTotalHour(getYesterday(date));
        HashMap map = new HashMap();
        map.put("yesterday", yesterdayHour);
        map.put("today", todayHour);
        Object o = JSONArray.toJSON(map);
        String result = o.toString();
        return result;
    }


    public String getYesterday(String today) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = format.parse(today);
            Date yes = DateUtils.addDays(date, -1);
            String yesterday = format.format(yes);
            return yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
