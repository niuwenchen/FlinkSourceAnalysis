package com.jackniu.flink.streaming.api;

/**
 * Created by JackNiu on 2019/6/6.
 */
public enum TimeCharacteristic {
    ProcessingTime,
    IntestionTime,
    EventTime;

    private TimeCharacteristic(){

    }
}
