package com.atguigu.gmall0826.mock.util;

/**
 * author : wuyan
 * create : 2020-02-06 14:12
 * desc :
 */
import java.util.Random;

public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

