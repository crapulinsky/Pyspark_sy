import os
import time
from pyspark.sql import SparkSession


'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_042stm1
    Author: SMOG
    Data: 2024/12/17
-------------------------------------------
'''
if __name__ == '__main__':
    # 程序开始运行
    start_time = time.time()
    # 设置环境变量
    os.environ['JAVA_HOME'] = 'E:/environment/java/jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = 'D:/Python/miniconda3/Lib/site-packages/pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = 'E:/Hadoop_local/hadoop-3.3.6'  # 如果需要 Hadoop，可选
    # 配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = 'D:/Python/miniconda3/python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/Python/miniconda3/python.exe'
    checkpoint_dir = "file:///E:/spark-temp/spark-checkpoints/stream-query" # checkpoint

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("t_042stm1") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation",checkpoint_dir) \
        .config("spark.log.level", "ERROR") \
        .getOrCreate()
    print(spark)
    # spark.createDataFrame()
    # static = spark.read.json("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/activity-data")
    static = spark.read.format("json") \
            .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/activity-data")

    dataschema = static.schema
    streaming = spark.readStream.schema(dataschema).option("maxFilesPerTrigger",10) \
                    .json("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/activity-data")

    activeCount = streaming.groupBy("gt").count()

    activityQuery = activeCount \
                    .writeStream \
                    .queryName("active_count") \
                    .format("memory").outputMode("complete") \
                    .start()
    try:
        while True:
            time.sleep(5)

            result = spark.sql("SELECT * FROM active_count")
            result.show()
    except KeyboardInterrupt:
        activityQuery.stop()

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)

    # for i in range(5):
    #     spark.sql('''
    #     select * from active_count
    #     ''').show()
    #     time.sleep(1)