package com.jackniu.flink.streaming.api.scala

/**
  * Created by JackNiu on 2019/6/6.
  */
import com.jackniu.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}

import scala.collection.JavaConverters._
class StreamingExecutionEnvironment(javaEnv: JavaEnv)  {

  def getJavaEnv: JavaEnv = javaEnv


}
