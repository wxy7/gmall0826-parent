package com.atguigu.gmall0826.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * author : wuyan
 * create : 2020-02-17 14:34
 * desc : 一个饼图
 */
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Options> options;
}
