import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
from common_utils.utils import get_spark_app_config
from common_utils.utils import load_survey_df,country_count,parse_gender



my_conf = get_spark_app_config()
# my_conf.set("spark.app.name","hello spark")
# my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()


survey_df = load_survey_df(spark,sys.argv[1])
partition_df = survey_df.repartition(2)
count_df = country_count(partition_df )

# column object expression
parse_gender_udf = udf(parse_gender,StringType())
transformed_df = survey_df.withColumn('Gender',parse_gender_udf('Gender'))
# transformed_df.show()

##sql expression
spark.udf.register('parse_gender_udf',parse_gender,StringType())
survey_df2 = survey_df.withColumn('Gender',expr('parse_gender_udf(Gender)'))
survey_df2.show(10)

### load flight_csv

# flight_csv = spark.read.format('parquet') \
#                       .option('path','data/flight*.parquet') \
#                       .load()
#
# flight_csv.show()


