import os
import time
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: _028etl1
    Author: SMOG
    Data: 2024/12/9
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

    # 创建 SparkSession，设置ojdbc驱动路径
    spark = SparkSession.builder \
        .appName("Oracle Read") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "E:/environment/java/ojdbc8.jar") \
        .config("spark.executor.extraClassPath", "E:/environment/java/ojdbc8.jar") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    # 创建jdbc
    jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl1"
    properties = {
        "user": "tester",
        "password": "123456",
        "driver": "oracle.jdbc.OracleDriver",
        "numPartitions" : "12",
        "partitionColumn": "id",
        "lowerBound": "1",
        "upperBound" : "104857600",
        "isolationLevel" : "NONE"
    }

    df = spark.read.format("csv") \
        .option("header","true" ) \
        .option("inferSchema","true") \
        .load("file:///E:/DB/data/test_data_random.csv")

    row_count = df.count()

    # 写进数据库
    try:
        df.write.jdbc(jdbc_url,"test_data_random",properties=properties, mode="overwrite")
        print(f"结果导入成功,总共{row_count}条数据")
    except Exception as e:
        print("结果导入失败:", str(e))
    finally:
        spark.stop()
        print("程序已关闭")

    # 程序结束
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)