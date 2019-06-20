package com.jackniu.flink.streaming.api.transformations;

/**
 * Created by JackNiu on 2019/6/17.
 */


public abstract class StreamTransformation<T> {

    // 定义一个unqiueID，给每一个StreamTransformation
    protected static Integer idCounter =0;

    public static int getNewNodeId(){
        idCounter ++;
        return idCounter;
    }

    protected  final int id;
    protected  String name;
    protected TypeInformation<T> outputType;



}
