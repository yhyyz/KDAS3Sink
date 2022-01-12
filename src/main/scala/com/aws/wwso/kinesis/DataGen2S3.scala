package com.aws.wwso.kinesis

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.wwso.kinesis.util.ParameterToolUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.GenericInMemoryCatalog
import org.slf4j.{Logger, LoggerFactory}

object DataGen2S3 {

  val LOG: Logger = LoggerFactory.getLogger(DataGen2Hudi.getClass)
  def main(args: Array[String]): Unit = {

    LOG.info(args.mkString(" ,"))
    // 解析命令行参数
    LOG.info("start run kda flink table api  datagen data to s3 ....")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    var parameter: ParameterTool = null
    if (env.getJavaEnv.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取参数 s3://app-util/flink-table-test-01/
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkTableAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkTableAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genDataGen2S3Params(parameter)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env,settings)
    // 设置Hive Catalog
    val catalogName = "datagen-memory"
    val database = "default"
    val memoryCatalog = new GenericInMemoryCatalog(catalogName,database)
    tEnv.registerCatalog(catalogName, memoryCatalog)
    tEnv.useCatalog(catalogName)

    val dataGenTable = params.dataGenTableName
    val sourceDataGenTableSQL =
      s"""
         |CREATE TEMPORARY table $dataGenTable(
         |  name VARCHAR
         |) WITH (
         |  'connector' = 'datagen'
         |)
         |""".stripMargin

    tEnv.executeSql(sourceDataGenTableSQL)
    val s3Table=params.s3TableName
    val createS3TableSQL=
      s"""CREATE TABLE  $s3Table"""+"""(
      |       name string
      |    ) with (
      |    'connector' = 'filesystem',
      |    'format' = 'parquet',
      |    'path' =""".stripMargin+s""" '${params.s3TablePath}'
      |    )
      """.stripMargin

    tEnv.executeSql(createS3TableSQL)

    // 流式插入数据
    val insertDataSQL=
      s"""
         |INSERT INTO  $s3Table SELECT
         |  name
         |FROM $dataGenTable
      """.stripMargin

    tEnv.executeSql(insertDataSQL)

  }

}
