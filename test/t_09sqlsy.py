from pyspark.sql import SparkSession
import os
import time

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
    #配置base环境python解释器的路径
    os.environ['PYSPARK_PYTHON'] = r'D:\Python\miniconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'D:\Python\miniconda3\python.exe'

    # 创建 SparkSession，设置ojdbc驱动路径
    sqlconf = SparkSession.builder \
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
    EMP_DF = sqlconf.read.jdbc(url=jdbc_url, table="EMP", properties=properties)
    DEPT_DF = sqlconf.read.jdbc(url=jdbc_url, table="DEPT", properties=properties)

    EMP_DF.createOrReplaceTempView("EMP")
    DEPT_DF.createOrReplaceTempView("DEPT")

    # sql
    sql_read = sqlconf.sql('''
    select d.dname,max(sal)
    from emp e
    join dept d
    on e.DEPTNO=d.DEPTNO
    group by d.dname
    order by max(sal)
   ''')
    # 打印结果和执行计划
    sql_read.show()
    sql_read.explain()

    sqlconf.stop()
    # 程序结束
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)