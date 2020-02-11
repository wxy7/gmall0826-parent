package com.atguigu.gmall0826.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-10 17:19
 * desc :
 */
public interface DauMapper {
    //查询每天新增日活总数
    public Long selectDauTotal(String date);

    //根据日期当天分段总数
    public List<Map> selectDauTotalHours(String date);
}
