package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.publisher.mapper.DauMapper;
import com.atguigu.gmall0826.publisher.mapper.OrderMapper;
import com.atguigu.gmall0826.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-10 18:30
 * desc :
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauTotalHour(String date) {
        List<Map> listMap = dauMapper.selectDauTotalHours(date);
        HashMap<String, Long> dauMap = new HashMap();
        for (Map map : listMap) {
            String logHour = (String)map.get("logHour");
            Long count = (Long)map.get("count");
            dauMap.put(logHour, count);
        }
        return dauMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHour(String date) {
        List<Map> list = orderMapper.selectOrderAmountHour(date);
        HashMap<String ,Double> hashMap = new HashMap<>();
        for (Map map : list) {
            String create_hour = (String) map.get("create_hour");
            System.out.println("create_hour:" +  create_hour);
            Double order_amount = (Double) map.get("order_amount");
            System.out.println("order_amount:" +  order_amount);
            hashMap.put(create_hour, order_amount);
        }
        return hashMap;
    }
}
