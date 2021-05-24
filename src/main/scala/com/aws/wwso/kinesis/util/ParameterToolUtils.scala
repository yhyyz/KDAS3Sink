package com.aws.wwso.kinesis.util

import com.aws.wwso.kinesis.model.ParamsModel
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties

object ParameterToolUtils {


  def fromApplicationProperties(properties: Properties): ParameterTool = {
    val map = new util.HashMap[String, String](properties.size())
    properties.forEach((k, v) => map.put(String.valueOf(k), String.valueOf(v)))
    ParameterTool.fromMap(map)
  }

  def genParams(parameter: ParameterTool): ParamsModel.Params = {
    val aws_region = parameter.get("aws_region")
    val ak = parameter.get("ak")
    val sk = parameter.get("sk")
    val inputStreamName = parameter.get("input_stream_name")
    val stream_init_position = parameter.get("stream_init_position")
    val streamInitialTimestamp = parameter.get("stream_initial_timestamp")
    val s3_output_path = parameter.get("s3_output_path")
    val params = ParamsModel.Params.apply(aws_region, ak, sk, inputStreamName, stream_init_position, streamInitialTimestamp, s3_output_path)
    params
  }
}
