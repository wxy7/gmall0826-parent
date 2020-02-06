package com.atguigu.gmall0826.mock.util;

/**
 * author : wuyan
 * create : 2020-02-06 14:12
 * desc :
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}

