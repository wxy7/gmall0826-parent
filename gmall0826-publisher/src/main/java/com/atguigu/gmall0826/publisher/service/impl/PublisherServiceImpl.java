package com.atguigu.gmall0826.publisher.service.impl;

import com.atguigu.gmall0826.publisher.mapper.DauMapper;
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
}
