package com.briup.base;

import java.util.ArrayList;
import java.util.List;

/**
 * 2 * @Author: Cai
 * 3 * @Date: 2019/7/5 15:56
 * 4
 */
public class Test {
    public static void main(String args[]){
        List<Integer> list = new ArrayList<>();
        list.add(new Integer(1));
        list.add(new Integer(1));
        list.add(new Integer(2));
        list.add(new Integer(3));

        list.forEach(System.out::println);
    }


}
