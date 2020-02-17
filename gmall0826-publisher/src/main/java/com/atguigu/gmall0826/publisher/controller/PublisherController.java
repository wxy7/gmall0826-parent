package com.atguigu.gmall0826.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.atguigu.gmall0826.publisher.bean.Options;
import com.atguigu.gmall0826.publisher.bean.Result;
import com.atguigu.gmall0826.publisher.bean.Stat;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.jcodings.util.Hash;
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

        Double orderAmount = publisherService.getOrderAmount(date);
        HashMap orderAmountMap = new HashMap();
        orderAmountMap.put("id", "order_amount");
        orderAmountMap.put("name", "新增交易额");
        orderAmountMap.put("value", orderAmount);
        list.add(orderAmountMap);

        String result = JSON.toJSONString(list);

        return result;
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        if("dau".equals(id)){
            Map<String, Long> todayHour = publisherService.getDauTotalHour(date);
            Map<String, Long> yesterdayHour = publisherService.getDauTotalHour(getYesterday(date));
            HashMap map = new HashMap();
            map.put("yesterday", yesterdayHour);
            map.put("today", todayHour);
            Object o = JSONArray.toJSON(map);
            String result = o.toString();
            return result;
        }else if("order_amount".equals(id)){
            Map<String, Double> todayHour = publisherService.getOrderAmountHour(date);
            Map<String, Double> yesterdayHour = publisherService.getOrderAmountHour(getYesterday(date));
//            System.out.println("todayHour:" + todayHour);
//            System.out.println("yesterdayHour:" + yesterdayHour);
            HashMap map = new HashMap();
            map.put("yesterday", yesterdayHour);
            map.put("today", todayHour);
            Object o = JSONArray.toJSON(map);
            String result = o.toString();
            return result;
        }
        return null;
    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int startpage,@RequestParam("size") int size,@RequestParam("keyword") String keyword) {
        if (date != "" && keyword != "") {
            System.out.println("====date===" + date);
            System.out.println("====keyword===" + keyword);
            Map saleDetailMap = publisherService.getSaleDetail(date, keyword, startpage, size);


            Long total = (Long) saleDetailMap.get("total");

            List detailList = (List) saleDetailMap.get("detailList");


            //年龄饼图
            HashMap ageAggMap = (HashMap) saleDetailMap.get("ageAggMap");
            Long age_20Count = 0L;
            Long age20_30Count = 0L;
            Long age30_Count = 0L;
            for (Object o : ageAggMap.keySet()) {
                String ageKey = (String) o;
                int age = Integer.parseInt(ageKey);
                if (age < 20) {
                    age_20Count += (Long) ageAggMap.get(ageKey);
                } else if (age >= 20 && age < 30) {
                    age20_30Count += (Long) ageAggMap.get(ageKey);
                } else {
                    age30_Count += (Long) ageAggMap.get(ageKey);
                }
            }
            Double age_20rate = 0D;
            Double age20_30rate = 0D;
            Double age30_rate = 0D;

            age_20rate = Math.round(age_20Count * 1000D / total) / 10D;
            age20_30rate = Math.round(age20_30Count * 1000D / total) / 10D;
            age30_rate = Math.round(age30_Count * 1000D / total) / 10D;

            List<Options> ageList = new ArrayList<>();
            ageList.add(new Options("20岁以下", age_20rate));
            ageList.add(new Options("20岁到30岁", age20_30rate));
            ageList.add(new Options("30岁及30岁以上", age30_rate));

            Stat ageStat = new Stat("用户年龄占比", ageList);


            //性别饼图
            HashMap genderAggMap = (HashMap) saleDetailMap.get("genderAggMap");
            Long female = (Long) genderAggMap.get("F");
            Long male = (Long) genderAggMap.get("M");

            Double femaleRate = 0D;
            Double maleRate = 0D;

            femaleRate = Math.round(female * 1000D / total) / 10D;
            maleRate = Math.round(male * 1000D / total) / 10D;


            List<Options> genderList = new ArrayList<>();
            genderList.add(new Options("男", maleRate));
            genderList.add(new Options("女", femaleRate));
            Stat genderStat = new Stat("用户性别占比", genderList);

            List<Stat> resultList = new ArrayList<>();
            resultList.add(ageStat);
            resultList.add(genderStat);

            Result result = new Result(total, detailList, resultList);
            return JSON.toJSONString(result);
        }
        return null;

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
