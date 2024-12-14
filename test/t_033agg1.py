import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import var_pop,var_samp,stddev_pop,stddev_samp

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_033agg1
    Author: SMOG
    Data: 2024/12/14
-------------------------------------------
'''
if __name__ == '__main__':
    # 程序开始运行
    start_time = time.time()
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    # 配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("{NAME}") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/retail-data/by-day/2010-12-01.csv") \
        .coalesce(5)
    df.cache()
    df.createOrReplaceTempView("dfTable")
    df.show()

    df.select(var_pop("Quantity"),var_samp("Quantity"),stddev_pop("Quantity"),stddev_samp("Quantity")).show()

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)