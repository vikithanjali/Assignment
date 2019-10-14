import json
import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('sample1.ini')
config.sections()

spark = SparkSession \
     .builder \
     .appName("Python Spark SQL basic example") \
     .config("spark.jars", "/home/fission/.ivy2/jars/mysql_mysql-connector-java-8.0.12.jar") \
     .getOrCreate()

dbURL = "jdbc:mysql://" + config['dev']['host'] + ":"+config['dev']['port'] + "/retail_db?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
print(dbURL)
df = spark.read.format("jdbc") \
     .options(url=dbURL,
              database=config['dev']['database'],
              dbtable='orders',
              user=config['dev']['user_name'],
              password=config['dev']['password'],
              driver='com.mysql.jdbc.Driver') \
     .load();
ct = df.groupBy(["order_date", "order_status"]).count()
op = ct.repartition(1).write.json(path="file:///home/fission/Desktop/jsonfile")























