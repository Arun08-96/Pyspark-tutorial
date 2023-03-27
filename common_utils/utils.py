
import  configparser
import re

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(r'G:\spark_demo\SparkProject\configs\spark.conf')

    for (key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)

    return spark_conf


## Reading the dataframe
def load_survey_df(spark, data_file):
    final_df = spark.read.format('csv') \
          .option('header',True) \
             .option('inferschema',True) \
          .load(data_file)

    return final_df

## Transformations
def country_count(dataframe):
    final_count = dataframe.where("Age < 40") \
        .select('Age', 'Country', 'state', 'tech_company') \
        .groupBy('country') \
        .count()

    return final_count


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern,gender.lower()):
        return "Female"
    elif re.search(male_pattern,gender.lower()):
        return "Male"
    else:
        return "Unknown"