import os
import time
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_040rdd2
    Author: SMOG
    Data: 2024/12/15
-------------------------------------------
'''
if __name__ == '__main__':
    #程序开始运行
    start_time = time.time()
    # 设置环境变量
    os.environ['JAVA_HOME'] = r'E:\environment\java\jdk-1.8'  # 设置 Java 路径
    os.environ['SPARK_HOME'] = r'D:\Python\miniconda3\Lib\site-packages\pyspark'  # 设置 Spark 路径
    os.environ['HADOOP_HOME'] = r'E:\Hadoop_local\hadoop-3.3.6'  # 如果需要 Hadoop，可选
    #配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("t_040rdd2") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    collection = "Here is a breakdown of the issue and possible resolution".split(" ")

    rdd1 = spark.sparkContext.parallelize(collection,2)

    rdd2 = rdd1.keyBy(lambda x : x.lower()[0])
    print(rdd2.collect())

    rdd3 = rdd2.mapValues(lambda x : x.upper())
    print(rdd3.collect())




    #程序结束
    spark.stop()
    end_time = time.time()
    #运行时间
    print("运行时间:",end_time - start_time)