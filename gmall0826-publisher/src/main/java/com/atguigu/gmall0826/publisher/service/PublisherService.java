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

    public Map<String,Long> getDauTotalHour(String date);
}
