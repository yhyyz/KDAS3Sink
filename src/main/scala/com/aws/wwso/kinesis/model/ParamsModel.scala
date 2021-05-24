package com.aws.wwso.kinesis.model

object ParamsModel {

  case class Params(awsRgeion: String, ak: String, sk: String, inputStreamName: String, streamInitPosition: String, streamInitialTimestamp: String, s3OutputPath: String)

}
