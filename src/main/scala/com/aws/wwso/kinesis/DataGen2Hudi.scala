package com.aws.wwso.kinesis

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.aws.wwso.kinesis.util.ParameterToolUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

object DataGen2Hudi {


  val LOG: Logger = LoggerFactory.getLogger(DataGen2Hudi.getClass)
  def main(args: Array[String]): Unit = {

    LOG.info(args.mkString(" ,"))
    // 解析命令行参数
    LOG.info("start run kda flink table api  datagen data to hudi ....")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 注意在Kinesis Analysis 运行时中该参数不生效，需要在CLI中设置相关参数，同时KDA 默认会使用RocksDB存储状态，不用设置
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    var parameter: ParameterTool = null
    if (env.getJavaEnv.getClass == classOf[LocalStreamEnvironment]) {
      parameter = ParameterTool.fromArgs(args)
    } else {
      // 使用KDA Runtime获取参数 s3://app-util/flink-table-test-01/
      val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties.get("FlinkTableHudiAppProperties")
      if (applicationProperties == null) {
        throw new RuntimeException("Unable to load properties from Group ID FlinkTableAppProperties.")
      }
      parameter = ParameterToolUtils.fromApplicationProperties(applicationProperties)
    }
    val params = ParameterToolUtils.genDataGen2HudiParams(parameter)

    // 创建 table env，流模式
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env,settings)
    // 设置Hive Catalog
//    val catalogName = "datagen-memory"
//    val database = "default"
//    val memoryCatalog = new GenericInMemoryCatalog(catalogName,database)
//    tEnv.registerCatalog(catalogName, memoryCatalog)
//    tEnv.useCatalog(catalogName)

    tEnv.executeSql("DROP TABLE IF EXISTS kda_hudi_tb_2012")
    val sourceTableSQL =
      s"""CREATE TABLE kda_hudi_tb_2012(
         |uuid string,
         |name string,
         |logday VARCHAR(255),
         |hh VARCHAR(255)
         |)PARTITIONED BY (`logday`,`hh`)
         |WITH (
         |  'connector' = 'hudi',
         |  'path' = 's3a://app-util/teck-talk/kda_hudi_tb_2012/',
         |  'table.type' = 'COPY_ON_WRITE',
         |  'write.precombine.field' = 'uuid',
         |  'write.operation' = 'upsert',
         |  'hoodie.datasource.write.recordkey.field' = 'uuid',
         |  'hive_sync.enable' = 'true',
         |  'hive_sync.use_jdbc' = 'false',
         |  'hive_sync.metastore.uris' = 'thrift://localhost:9083',
         |  'hive_sync.table' = 'kda_hudi_tb_2012',
         |  'hive_sync.mode' = 'HMS',
         |  'hive_sync.username' = 'hadoop',
         |  'hive_sync.partition_fields' = 'logday,hh',
         |  'properties.hoodie.embed.timeline.server=false' = 'false',
         |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
         |)
         |
      """.stripMargin
    tEnv.executeSql(sourceTableSQL)
    LOG.info("gen2hudi table ")
    //insert data to hive(s3)
    val insertDataSQL =
      s"""
         |insert into  kda_hudi_tb_2012 select '1' as uuid,'customer' as name,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') as logday, DATE_FORMAT(CURRENT_TIMESTAMP, 'hh') as hh
      """.stripMargin

    tEnv.executeSql(insertDataSQL)

  }


}
