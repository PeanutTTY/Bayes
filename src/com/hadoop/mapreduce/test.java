package com.hadoop.mapreduce;

import java.io.File;
import java.util.ArrayList;


public class test {
    public static void main(String[] args){
        String s0 = "Hello";
        String s1 = "world";
        String s2 = s0+" "+s1;
        if(s2 == s0){
            System.out.println(true);
        }
        else {
            System.out.println(false);
        }
    }

}
