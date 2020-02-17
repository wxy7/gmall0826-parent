package com.atguigu.gmall0826.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * author : wuyan
 * create : 2020-02-17 14:33
 * desc : 最终的结果
 */
@Data
@AllArgsConstructor
public class Result {
    Long total;
    List<Map> detailList;
    List<Stat> stat;
}
