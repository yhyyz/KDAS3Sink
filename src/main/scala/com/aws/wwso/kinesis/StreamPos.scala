package com.aws.wwso.kinesis

object StreamPos extends Enumeration {

  type StreamPos = Value
  //枚举的定义
  val LATEST, TRIM_HORIZON, AT_TIMESTAMP = Value
}
