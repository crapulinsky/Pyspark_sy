from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import date_format,col,datediff,current_date

'''
-------------------------------------------
    Description: TODO:
    SourceFile: _09spark
    Author: SMOG
    Data: 2024/11/29
-------------------------------------------
'''
if __name__ == '__main__':
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
        .config("spark.driver.extraClassPath", "E:/environment/java/ojdbc8.jar") \
        .config("spark.executor.extraClassPath", "E:/environment/java/ojdbc8.jar") \
        .getOrCreate()

    # 创建jdbc
    jdbc_url = "jdbc:oracle:thin:@localhost:1521:orcl1?characterEncoding=UTF-8"
    properties = {
        "user": "tester",
        "password": "123456",
        "driver": "oracle.jdbc.OracleDriver"
    }

    # 创建临时视图,记得改表名
    test_table = spark.read.jdbc(url=jdbc_url, table="test_table", properties=properties)


    # sql
    sql_read = test_table.select(
        "id",
        date_format(col("DATE_COL"), 'yyyy-MM').alias("yearmonth"),  # 使用正确的日期格式
        datediff(current_date(), col("DATE_COL")).alias("days_diff")  # 计算当前日期与 DATE_COL 的差异（天数）
    )

    # 打印结果
    sql_read.show()
    sql_read.printSchema()
    spark.stop()
