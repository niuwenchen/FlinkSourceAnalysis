package com.jackniu.flink.streaming.api;

/**
 * Created by JackNiu on 2019/6/17.
 */

/**
 * checkpoinging 模式定义一致性 确保系统当出现故障时，自动停止工作
 *当检查点被激活时，数据流重新运行，从而处理丢失的部分。对于有状态的操作和函数，检查点模式定义系统是否绘制检查点，以便恢复的行为就像操作符函数签好一次
 *  检查每个记录一样，或者检查点用一种简单的方式绘制，但在恢复的时候回出现一些重复
 *
 */

public enum CheckpointingMode {
    EXACTLY_ONCE,
    AT_LEAST_ONCE
}
