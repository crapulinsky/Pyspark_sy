import os
import time
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import DoubleType
'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_054T3
    Author: SMOG
    Data: 2024/12/25
-------------------------------------------
'''
def crossTabs(scored: DataFrame, t: DoubleType) -> DataFrame:
  return  scored.selectExpr(f"score >= {t} as above", "is_match").\
           groupBy("above").pivot("is_match", ("true", "false")).\
          count()
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
        .appName("t_054T3") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    schema = "Name String,Age Int,Score Int"

    data = [
        ("Alice", 23, 90),
        ("Bob", 24, 85),
        ("Cathy", 22, 88),
        ("David", 23, 92),
        ("Eve", 24, 78)
    ]

    listdf = spark.createDataFrame(data, schema)

    listdf.show()

    crossTabs(listdf,80)


    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)