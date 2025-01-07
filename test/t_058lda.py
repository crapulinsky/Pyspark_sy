import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_058lda
    Author: SMOG
    Data: 2024/12/28
-------------------------------------------
'''

if __name__ == '__main__':
    # 程序开始运行
    start_time = time.time()

    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-11'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    checkpoint_dir = "file:///E:/spark-temp/spark-checkpoints/stream-query"  # checkpoint

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("t_058lda") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 5) \
        .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
        .getOrCreate()

    # 读取静态数据作为参考
    static = spark.read.format("json") \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/activity-data")

    # 定义流数据源
    streaming = spark.readStream \
        .schema(static.schema) \
        .option("maxFilesPerTrigger", 10) \
        .json("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/activity-data")

    # 打印数据结构以确保数据正确
    streaming.printSchema()

    # 添加事件时间并进行分组
    with_event_time = streaming.selectExpr(
        "*",
        "cast(cast(Creation_Time as double)/100000000 as timestamp) as event_time"
    )

    # 对事件时间进行窗口分组
    query = with_event_time.groupBy(window(col("event_time"), "10 minutes","5 minutes")).count() \
        .withWatermark("10 minutes") \
        .writeStream \
        .queryName("xxx")\
        .format("memory") \
        .outputMode("complete") \
        .start()

    # 等待一段时间以确保数据写入内存表
    time.sleep(5)  # 可以根据需求调整

    # 使用 try-except 块以便捕获中断
    try:
        while True:
            # 每 5 秒查询一次内存表 "xxx"
            time.sleep(5)
            result = spark.sql("SELECT * FROM xxx")  # 查询内存表
            result.show(truncate=False)  # 显示结果
    except KeyboardInterrupt:
        query.stop()  # 停止流查询

    # 等待流查询终止
    query.awaitTermination()  # 使程序一直等待，直到流查询结束

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
