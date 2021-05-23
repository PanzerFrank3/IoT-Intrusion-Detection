import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

class Configuration():
	def restore_Configuration_Dataframe():

	  RANDOM_SEED = 42 
	  conf = SparkConf().set("spark.ui.port", "4050").set('spark.executor.memory', '4G').set('spark.driver.memory', '45G').set('spark.driver.maxResultSize', '10G')

	  sc = pyspark.SparkContext(conf=conf)
	  spark = SparkSession.builder.getOrCreate()

	  spark
	  sc._conf.getAll()

	  net_traffic_df = spark.read.load("/content/drive/MyDrive/Preprocessed_data.csv.bz2", 
		                         format="csv", 
		                         sep=",", 
		                         inferSchema="true", 
		                         header="true"
		                         )
	  
	  # I rename the columns to avoid errors due to the "." and rename the column normality in label 
	  net_traffic_renamed = (net_traffic_df.withColumnRenamed("normality", "label")
		                             .withColumnRenamed("frame.number", "frame_num")
		                             .withColumnRenamed("frame.time", "frame_t")
		                             .withColumnRenamed("frame.len", "frame_len")
		                             .withColumnRenamed("eth.src", "eth_src")
		                             .withColumnRenamed("eth.dst", "eth_dst")
		                             .withColumnRenamed("ip.src", "ip_src")
		                             .withColumnRenamed("ip.dst", "ip_dst")
		                             .withColumnRenamed("ip.proto", "ip_proto")
		                             .withColumnRenamed("ip.len", "ip_len")
		                             .withColumnRenamed("tcp.len", "tcp_len")
		                             .withColumnRenamed("tcp.srcport", "tcp_srcport")
		                             .withColumnRenamed("tcp.dstport", "tcp_dstport"))
	  
	  # I take the columns containing the features and put them in a variable to be used for subsequent models
	  inputsCols = net_traffic_renamed.select([c for c in net_traffic_renamed.columns if c not in {'label'}])

	  return RANDOM_SEED, sc, spark, net_traffic_df, net_traffic_renamed, inputsCols
