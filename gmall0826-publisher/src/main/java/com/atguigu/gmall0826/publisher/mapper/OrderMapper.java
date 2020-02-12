package com.atguigu.gmall0826.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-12 10:49
 * desc :
 */
public interface OrderMapper {
    public Double selectOrderAmount(String date);

    public List<Map> selectOrderAmountHour(String date);
}
