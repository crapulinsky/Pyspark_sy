import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType,StructType
from pyspark.sql.functions import col

'''
-------------------------------------------
    Description: TODO:
    SourceFile: _027null2
    Author: SMOG
    Data: 2024/12/8
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
        .appName("{NAME}") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    schema = StructType(
        [StructField("name",StringType(),True),
         StructField("age",StringType(),True)])

    date=[("bob",12),("alice",20),("bill",None),("david",None)]

    df = spark.createDataFrame(date,schema)
    # 返回ture/false
    # df.select(
    #     col("age").eqNullSafe(None).alias("age_is_null"),
    #     col("name")
    # ).show()

    df.na.drop("all",subset=["age"]).show()


    spark.stop()
    #程序结束
    end_time = time.time()
    #运行时间
    print("运行时间:",end_time - start_time)