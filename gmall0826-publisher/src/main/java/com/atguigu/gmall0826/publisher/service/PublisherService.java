package com.atguigu.gmall0826.publisher.service;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-10 18:28
 * desc :
 */
public interface PublisherService {
    public Long getDauTotal(String date);

    public Map<String, Long> getDauTotalHour(String date);

    public Double getOrderAmount(String date);

    public Map<String, Double> getOrderAmountHour(String date);

    //从es中查询指定日期和关键词的数据
    public Map getSaleDetail(String date, String keyword, int pageNo, int pageSize);
}