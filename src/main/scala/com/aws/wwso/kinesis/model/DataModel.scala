package com.aws.wwso.kinesis.model

object DataModel {

  // Kinesis Stream数据为JSON，两个字段,样例 {"deviceId":"a8xrtuyx","name":"aws-data"}
  case class Data(deviceId: String, name: String)

}
