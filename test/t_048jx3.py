import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_048jx3
    Author: SMOG
    Data: 2024/12/21
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
        .appName("t_048jx3") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    data1 = [
        (1, "Alice", "2023-12-01", 100),
        (2, "Bob", "2023-11-15", 200),
        (3, "Cathy", "2023-10-10", 150),
        (4, "David", "2023-09-25", 300)
    ]
    schema1 = ["OrderID", "CustomerName", "OrderDate", "Amount"]
    hisdf = spark.createDataFrame(data1, schema1)

    data2 = [
        (3, "Cathy", "2023-10-10", 150),  # 重复订单
        (5, "Eve", "2023-12-05", 250),  # 新增订单
        (6, "Frank", "2023-12-10", 300)  # 新增订单
    ]
    schema2 = ["OrderID", "CustomerName", "OrderDate", "Amount"]
    newdf = spark.createDataFrame(data2, schema2)

    joinCondition = hisdf["OrderID"] == newdf["OrderID"]
    hisdf.join(newdf,joinCondition,"cross" ).show()

    # hisdf.show()
    # newdf.show()

    #程序结束
    spark.stop()
    end_time = time.time()
    #运行时间
    print("运行时间:",end_time - start_time)