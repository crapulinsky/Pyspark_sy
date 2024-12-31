import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_056cjk
    Author: SMOG
    Data: 2024/12/27
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
        .appName("t_056cjk") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    left_data = [
        (1, "Alice", 100.0),
        (2, "Bob", 200.0),
        (3, "Charlie", 150.0),
        (4, "David", 180.0),
        (5, "Eve", 220.0)
    ]
    right_data = [
        (1, "Alice", 50.0),
        (2, "Bob", 80.0),
        (6, "Frank", 120.0)
    ]

    columns = ["id", "name", "value"]

    left_df = spark.createDataFrame(left_data, schema=columns)
    right_df = spark.createDataFrame(right_data, schema=columns)

    # 加了一个列source作为来源
    left_df = left_df.withColumn("source", lit("left"))
    right_df = right_df.withColumn("source", lit("right"))

    left_df.show()
    right_df.show()
    left_df.createOrReplaceTempView("left_data")
    right_df.createOrReplaceTempView("right_data")

    spark.sql('''
    SELECT id, name, value, source
    FROM (
        SELECT 
            COALESCE(l.id, r.id) AS id,
            l.name AS name,
            l.value AS value,
            'left' AS source
        FROM left_data l
        FULL OUTER JOIN right_data r ON l.id = r.id
        UNION ALL
        SELECT 
            COALESCE(l.id, r.id) AS id,
            r.name AS name,
            r.value AS value,
            'right' AS source
        FROM left_data l
        FULL OUTER JOIN right_data r ON l.id = r.id
    ) combined_data
    ORDER BY id, source;
    ''').show()

    # union_df = left_df.union(right_df)
    #
    # result_df = union_df.orderBy(["id", "source"], ascending=[True, True])
    # result_df.show()

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)

