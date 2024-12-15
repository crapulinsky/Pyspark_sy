import os
import time
from pyspark.sql import SparkSession

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_038join1
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
        .appName("t_038join1") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)

    person = spark.createDataFrame([
        (0,"Bill Chambers",0,[100]),
        (1,"Matei Zaharia",1,[500,250,100]),
        (2,"Michael Armbrust",1,[100])
    ]).toDF("id","name","graduate_program","spark_status")

    graduateProgram = spark.createDataFrame([
        (0,"Master","School of Information","UC Berkeley"),
        (1, "Master", "EECS", "UC Berkeley"),
        (2, "Ph.D.", "EECS", "UC Berkeley")
    ]).toDF("id","degree","department","school")

    sparkStatus = spark.createDataFrame([
        (500,"Vice President"),
        (250,"PMC Member"),
        (100,"Contributor"),
    ]).toDF("id","status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")

    joinExoression = person["graduate_program"] == graduateProgram["id"]

    person.join(graduateProgram,joinExoression,"left_anti").show()


    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
