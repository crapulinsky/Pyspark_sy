import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col,expr,lit
'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_046jx
    Author: SMOG
    Data: 2024/12/21
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
        .appName("t_052T1") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    def pivot_summary(desc):
        desc_p = desc.toPandas()

        desc_p = desc_p.set_index('summary').transpose().reset_index()
        desc_p = desc_p.rename(columns={'index': 'field'})
        # desc_p = desc_p.rename_axis(None, axis=1)

        descT = spark.createDataFrame(desc_p)

        for c in descT.columns:
            if c == 'field':
                continue
            else:
                descT = descT.withColumn(c, descT[c].cast(DoubleType()))
        return descT

    # schema = StructType([
    #     StructField("Name", StringType(), True),
    #     StructField("Age", IntegerType(), True),
    #     StructField("Score", IntegerType(), True)
    # ])

    schema = "Name String,Age Int,Score Int"

    data = [
        ("Alice", 23, 90),
        ("Bob", 24, 85),
        ("Cathy", 22, 88),
        ("David", 23, 92),
        ("Eve", 24, 78)
    ]

    listdf  = spark.createDataFrame(data, schema).describe().select("summary","Age","Score")

    listdf.show()

    list_t = pivot_summary(listdf)
    list_t.show()


    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
# ,col("Name"),col("Score"),expr("Name"),expr("Score")
