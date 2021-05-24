#### KDA消费KDS数据写入S3
1. 主函数Streaming2S3
2. 实现自定义Bucket，也就是自定义输入到S3的目录格式，抽取日志中的某个字段值作为分区目录

#### 本地调试参数
```
-aws_region 你的region -ak 你的AK -sk 你的SK
-input_stream_name KDA名字
-stream_init_position 消费位置
-s3_output_path 输出到S3目录

# 注意本地调试时，由于要输出到S3,需要在程序环境变量中加入
AWS_ACCESS_KEY_ID= 你的AK
AWS_SECRET_ACCESS_KEY= 你的SK
```
#### 部署到KDA参数
```
# 在KDA Console配置或者CLI都可以
Group: FlinkAppProperties
Key: aws_region
Value: 你的region
Key: input_stream_name
Value: KDA Steaming名字
Key: s3_output_path
Value: 输出路径
Key: stream_init_position
Value: 消费位置

```
#### build
```
 mvn clean package -Dscope.type=provided
```
