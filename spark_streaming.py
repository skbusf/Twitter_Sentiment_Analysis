from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
from textblob import TextBlob
import pyspark.sql.types as T


#kafka topic config
kafka_topic = 'big_data_project'
kafka_bootstrap_server = 'localhost:9092'

#mysql config
mysql_host = 'localhost'
mysql_port = '3306'
mysql_db_name = 'big_data_project'
mysql_table_name = 'tweets'
mysql_driver_class = 'com.mysql.jdbc.Driver'
mysql_username = 'root'
mysql_password = 'password'
mysql_jdbc_url = 'jdbc:mysql://' + mysql_host + ':' + mysql_port + '/' + mysql_db_name

#this method writed the data to mysql repo
def write_to_mysql(df, epoch_id):
    df.na.drop() \
        .write.format("jdbc") \
        .mode("append") \
        .option("url", mysql_jdbc_url) \
        .option("dbtable", mysql_table_name) \
        .option("user", mysql_username) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver_class) \
        .save()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KafkaConsumer") \
        .master("local[*]") \
        .config("spark.jars", "file:///C:/Users/batch_2kjxgc7/Desktop/big_data_project/mysql-connector-java-8.0.28.jar") \
        .config("spark.executor.extraClassPath", "file:///C:/Users/batch_2kjxgc7/Desktop/big_data_project/mysql-connector-java-8.0.28.jar") \
        .config("spark.executor.extraLibrary", "file:///C:/Users/batch_2kjxgc7/Desktop/big_data_project/mysql-connector-java-8.0.28.jar") \
        .config("spark.driver.extraClassPath", "file:///C:/Users/batch_2kjxgc7/Desktop/big_data_project/mysql-connector-java-8.0.28.jar") \
        .config("spark.mongodb.input.uri", "mongodb://hduser:bigdata@127.0.0.1:27017/dezyre") \
        .config("spark.mongodb.output.uri", "mongodb://hduser:bigdata@127.0.0.1:27017/dezyre") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
        

    spark.sparkContext.setLogLevel("Error")

    #reading the data from kafka topic
    tweets = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
            .option("subscribe", kafka_topic) \
            .option("startingoffsets", "latest") \
            .load()

    # #printing the schema of the tweets
    # print("Printing Schema of Tweets: ")
    # tweets.printSchema()

    # #casting the key and value columns to string
    stream_detail_df = tweets.selectExpr('CAST(key AS STRING)','CAST(value AS STRING)', 'timestamp')

    # schema of the data in the kafka topic
    tweets_schema = StructType() \
        .add("date", StringType()) \
        .add("id", StringType()) \
        .add("content", StringType()) \
        .add("username", StringType()) \
        .add("like_count", StringType()) \
        .add("retweet_count", StringType())
    
    # parsing the data from kafka topic
    tweets_df_1 = stream_detail_df.select(from_json(col("value").cast("string"), tweets_schema).alias("tweets"), "timestamp")

    # unpacking the data from kafka topic
    tweets_df = tweets_df_1.select("tweets.*", "timestamp")

    # {'date': '2023-03-29 21:00:48+00:00', 
    #  'id': '1641183648375525376', 
    #  'content': "replace all your chat GPT bots with me, I promise you'll get better results ≡ƒÆ£", 
    #  'username': 'LilyLyco', 
    #  'like_count': 5.0, 
    #  'retweet_count': 0.0}


    # print('Printing Schema of tweets_df: ')
    # tweets_df.printSchema()

    # Text cleaning function
    def cleanTweet(tweet: str) -> str:
        tweet = re.sub(r'http\S+', '', str(tweet))
        tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
        tweet = tweet.strip('[link]')

        # Remove users
        tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
        tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

        # Remove punctuation
        my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
        tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

        # Remove numbers
        tweet = re.sub('([0-9]+)', '', str(tweet))

        # Remove hashtags
        tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

        # Remove extra symbols
        tweet = re.sub('@\w+', '', str(tweet))
        tweet = re.sub('\n', '', str(tweet))

        return tweet

    # TextBlob Subjectivity function
    def Subjectivity(tweet: str) -> float:
        return TextBlob(tweet).sentiment.subjectivity
    
    # TextBlob Polarity function
    def Polarity(tweet: str) -> float:
        return TextBlob(tweet).sentiment.polarity

    # Assign sentiment to elements
    def Sentiment(polarityValue: int) -> str:
        if polarityValue < 0:
            return 'Negative'
        elif polarityValue == 0:
            return 'Neutral'
        else:
            return 'Positive'

    # User defined function creation from normal functions
    clean_tweets_udf = udf(cleanTweet, T.StringType())
    subjectivity_func_udf = udf(Subjectivity, T.FloatType())
    polarity_func_udf = udf(Polarity, T.FloatType())
    sentiment_func_udf = udf(Sentiment, T.StringType())

    # Tweet processing
    cl_tweets = tweets_df.withColumn('processed_text', clean_tweets_udf(col("content")))
    subjectivity_tw = cl_tweets.withColumn('subjectivity', subjectivity_func_udf(col("processed_text")))
    polarity_tw = subjectivity_tw.withColumn("polarity", polarity_func_udf(col("processed_text")))
    sentiment_tw = polarity_tw.withColumn("sentiment", sentiment_func_udf(col("polarity")))

    # printing the processed data to console
    tweets_stream = sentiment_tw.writeStream \
    .outputMode("append") \
    .trigger(processingTime="10 second") \
    .format("console") \
    .start()

    # writing the processed data to mysql
    tweets_stream = sentiment_tw.writeStream.outputMode("append").trigger(processingTime="10 second").foreachBatch(write_to_mysql).start()

    tweets_stream.awaitTermination() 