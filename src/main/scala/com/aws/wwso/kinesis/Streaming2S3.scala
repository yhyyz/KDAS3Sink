/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aws.wwso.kinesis

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants

import java.util.Properties
import com.google.gson.GsonBuilder
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.wwso.kinesis.model.{DataModel, ParamsModel}
import com.aws.wwso.kinesis.util.ParameterToolUtils
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.logging.log4j.LogManager
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Steaming to S3
 */
object Streaming2S3 {

  private val log = LogManager.getLogger(Streaming2S3.getClass)
  private val gson = new GsonBuilder().create

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // ?????????Kinesis Analysis ??????????????????????????????????????????CLI??????????????????????????????KDA ???????????????RocksDB???????????????????????????
    env.enableCheckpointing(5000)
    var parameter: ParameterTool = null
    if (env.getJavaEnv.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // ??????KDA Runtime????????????
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genParams(parameter)
    val source = createSourceFromStaticConfig(env, params)
    log.info("Kinesis stream created.")
    source.map(line => {
      try {
        val event: DataModel.Data = gson.fromJson(line, classOf[DataModel.Data])
        event
      } catch {
        case e: Exception => {
          log.error(e.getMessage)
        }
          DataModel.Data("error", "error")
      }
    }).filter(_.name != null)
      .addSink(createSinkFromStaticConfig(params)).setParallelism(1)
    log.info("S3 Sink added.")
    env.execute("kinesis analytics from data stream to s3")
  }

  def createSourceFromStaticConfig(env: StreamExecutionEnvironment, params: ParamsModel.Params): DataStream[String] = {
    val consumerConfig = new Properties()
    consumerConfig.put(AWSConfigConstants.AWS_REGION, params.awsRgeion)
    // ??????????????????AKSK???????????????????????????
    if (params.ak != null && params.sk != null) {
      consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, params.ak)
      consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, params.sk)
    }
    // ?????????????????????
    consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, params.streamInitPosition)
    if (params.streamInitPosition.equalsIgnoreCase(StreamPos.AT_TIMESTAMP.toString)) {
      consumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP, params.streamInitialTimestamp)
    }
    val kinesis = env.addSource(new FlinkKinesisConsumer[String](
      params.inputStreamName, new SimpleStringSchema(), consumerConfig))
    kinesis
  }

  def createSinkFromStaticConfig(params: ParamsModel.Params) = {
    val sink = StreamingFileSink
      .forBulkFormat(
        new Path(params.s3OutputPath),
        ParquetAvroWriters.forReflectRecord(classOf[DataModel.Data])
      ).withBucketAssigner(new CustomBucketAssigner)
      .build()
    sink
  }

}
