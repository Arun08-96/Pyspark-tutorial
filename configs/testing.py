from pyspark import SparkConf
import configs
import  configparser


# spark_conf = SparkConf()
# config = configparser.ConfigParser()
# config.read("spark.conf")
#
# print(config["SPARK_APP_CONFIGS"]['spark.app.name'])

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(r'G:\spark_demo\SparkProject\configs\spark.conf')

    for (key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)

    return spark_conf
#
# get_spark_app_config()