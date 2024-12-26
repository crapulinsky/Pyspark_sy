import os
import time
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_053sy2
    Author: SMOG
    Data: 2024/12/25
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
        .appName("t_053sy2") \
        .master("local[*]") \
        .getOrCreate()
    print(spark)



    # Load training data
    training = spark.read.format("libsvm").load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/sample_libsvm_data.txt")

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))

    # We can also use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8,
                             family="multinomial")

    # Fit the model
    mlrModel = mlr.fit(training)

    # Print the coefficients and intercepts for logistic regression
    # with multinomial family
    print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
    print("Multinomial intercepts: " + str(mlrModel.interceptVector))

    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
