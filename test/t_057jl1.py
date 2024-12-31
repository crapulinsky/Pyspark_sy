import os
import time
from pprint import pprint
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from random import randint

'''
-------------------------------------------
    Description: TODO:
    SourceFile: t_057jl1
    Author: SMOG
    Data: 2024/12/28
-------------------------------------------
'''
def clustering_score(input_data,k):
    input_numeric_only = input_data.drop("protocol_type", "service", "flag")
    assembler = VectorAssembler().setInputCols(input_numeric_only.columns[:-1]) \
        .setOutputCol("featureVector")
    kmeans = KMeans().setSeed(randint(100,100000)).setK(k) \
                    .setPredictionCol("cluster") \
                    .setFeaturesCol("featureVector")
    pipeline = Pipeline(stages = [assembler,kmeans])
    pipeline_model = pipeline.fit(input_numeric_only)

    evaluator = ClusteringEvaluator(predictionCol="cluster",
                                    featuresCol="featureVector")

    predictions = pipeline_model.transform(numeric_only)
    score = evaluator.evaluate(predictions)
    return score


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
        .appName("t_057jl1") \
        .master("local[*]") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    print(spark)

    # 准备数据集
    data_without_header = spark.read.format("csv") \
                            .option("inferSchema","true") \
                            .option("header","false") \
                            .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/kddcup.data.corrected")

    column_names = ["duration", "protocol_type", "service", "flag",
                    "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
                    "hot", "num_failed_logins", "logged_in", "num_compromised",
                    "root_shell", "su_attempted", "num_root", "num_file_creations",
                    "num_shells", "num_access_files", "num_outbound_cmds",
                    "is_host_login", "is_guest_login", "count", "srv_count",
                    "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
                    "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
                    "dst_host_count", "dst_host_srv_count",
                    "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
                    "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
                    "dst_host_serror_rate", "dst_host_srv_serror_rate",
                    "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
                    "label"]

    data = data_without_header.toDF(*column_names)
    numeric_only = data.drop("protocol_type", "service", "flag")

    # # 管道
    # assembler = VectorAssembler().setInputCols(numeric_only.columns[:-1]) \
    #                             .setOutputCol("featureVector")
    # kmeans = KMeans().setPredictionCol("cluster").setFeaturesCol("featureVector")
    # pipeline = Pipeline().setStages([assembler,kmeans])
    #
    #
    # # 训练
    # pipeline_model = pipeline.fit(numeric_only)
    # kmeans_model = pipeline_model.stages[1]
    # pprint(kmeans_model)

    for k in list(range(20,100,20)):
        print(f"{k}:",clustering_score(numeric_only,k))




    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
