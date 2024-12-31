import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast,when,col,sum
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.ml.recommendation import ALS
'''
-------------------------------------------
    Description: 音乐数据推荐算法
    SourceFile: m1_music
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
            .appName("m1_music") \
            .master("local[*]") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
    print(spark)

    # 数据结构
    Schema1 = StructType([
        StructField("user",IntegerType()),
        StructField("artist",IntegerType()),
        StructField("count",IntegerType())
    ])

    Schema2 = StructType([
        StructField("artist", IntegerType()),
        StructField("alias", StringType())
    ])

    Schema3 = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType())
    ])

    # 原始数据导数
    user_artist_df = spark.read.format("csv") \
                .option("header","false") \
                .option("inferSchema","true") \
                .option("delimiter", " ") \
                .schema(schema=Schema1) \
                .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/profiledata_06-May-2005/user_artist_data.txt")
    user_artist_df.show()

    artist_alias = spark.read.format("csv") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .option("delimiter", "\t") \
        .schema(schema=Schema2) \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/profiledata_06-May-2005/artist_alias.txt")
    artist_alias.show()

    artist_ID = spark.read.format("csv") \
        .option("header", "false") \
        .option("inferSchema", "true") \
        .option("delimiter", "\t") \
        .schema(schema=Schema3) \
        .load("file:///D:/XXX/PycharmProjects/Pyspark_sy/data/profiledata_06-May-2005/artist_data.txt")
    artist_ID.show()
    # 训练数据集
    train_data = user_artist_df.join(broadcast(artist_alias),'artist','left') \
                                .withColumn('artist',when(col('alias').isNull(),col('artist'))
                                            .otherwise(col('alias'))) \
                                .withColumn('artist', col('artist')\
                                             .cast(IntegerType()))\
                                             .drop('alias')
    train_data.show()

    # 调用Spark自带ALS模型
    model = ALS(rank=10, seed=0, maxIter=5, regParam=0.1,
                implicitPrefs=True, alpha=1.0, userCol='user',
                itemCol='artist', ratingCol='count'). \
        fit(train_data)

    model.userFactors.show(1, truncate=False)

    # 向用户2093760推荐
    user_id = 2093760
    # 查看他喜欢什么
    existing_artist_ids = train_data.filter(train_data.user == user_id) \
    .select("artist").collect()
    # 展示喜好
    existing_artist_ids = [i[0] for i in existing_artist_ids]
    artist_ID.filter(col('id').isin(existing_artist_ids)).show()

    user_subset = train_data.select('user').where(col('user')==user_id).dropDuplicates()
    top_predictions = model.recommendForUserSubset(user_subset,5)

    top_predictions.show()

    top_predictions_pandas = top_predictions.toPandas()
    print(top_predictions_pandas)
    # 查看哪些不推荐
    recommended_artist_ids = [i[0] for i in top_predictions_pandas.\
                                        recommendations[0]]
    artist_ID.filter(col('id').isin(recommended_artist_ids)).show()

    def area_under_curve(
            positive_data,
            b_all_artist_IDs,
            predict_function):
        ...
    all_data = user_artist_df.join(broadcast(artist_alias), 'artist', how='left') \
        .withColumn('artist', when(col('alias').isNull(), col('artist')) \
                    .otherwise(col('alias'))) \
        .withColumn('artist', col('artist').cast(IntegerType())).drop('alias')

    train_data, cv_data = all_data.randomSplit([0.9, 0.1], seed=54321)
    train_data.cache()
    cv_data.cache()

    all_artist_ids = all_data.select("artist").distinct().count()
    b_all_artist_ids = broadcast(all_artist_ids)

    model = ALS(rank=10, seed=0, maxIter=5, regParam=0.1,
                implicitPrefs=True, alpha=1.0, userCol='user',
                itemCol='artist', ratingCol='count') \
        .fit(train_data)
    area_under_curve(cv_data, b_all_artist_ids, model.transform)

    from pyspark.sql.functions import sum as _sum


    def predict_most_listened(train):
        listen_counts = train.groupBy("artist") \
            .agg(_sum("count").alias("prediction")) \
            .select("artist", "prediction")

        return all_data.join(listen_counts, "artist", "left_outer"). \
            select("user", "artist", "prediction")


    area_under_curve(cv_data, b_all_artist_ids, predict_most_listened(train_data))


    # 程序结束
    spark.stop()
    end_time = time.time()
    # 运行时间
    print("运行时间:", end_time - start_time)
