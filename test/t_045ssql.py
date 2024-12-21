import os
import time
from pyspark.sql import SparkSession


'''
-------------------------------------------
    Description: TODO:
    SourceFile: _09spark
    Author: SMOG
    Data: 2024/11/29
-------------------------------------------
'''
if __name__ == '__main__':
    #程序开始运行
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
        .appName("t_045ssql DBRead") \
        .config("spark.driver.extraClassPath", "E:environment/java/ojdbc8.jar") \
        .config("spark.executor.extraClassPath", "E:/environment/java/ojdbc8.jar") \
        .getOrCreate()
    
    #创建jdbc
    jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl1"

    # schema = StructType([
    #     StructField("EMP_ID",DecimalType(38,10),True),
    #     StructField("EMP_NAME",StringType(),True),
    #     StructField("DEPT_ID",DecimalType(38,10),True)
    # ])
    
    #创建临时视图,记得改表名
    try:
        test_qc = spark.read.format("jdbc") \
            .option("url",jdbc_url) \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("user","tester") \
            .option("password","123456") \
            .option("dbtable", "test_qc") \
            .option("inferSchema",True) \
            .load()
    except Exception as e:
        print(e)

    df1 = test_qc.dropDuplicates() # 去重
    df1.show()

    
    #程序结束
    spark.stop()
    end_time = time.time()
    #运行时间
    print("运行时间:",end_time - start_time)
