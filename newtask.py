import json
import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.sections()
config.read('sample.ini')
# ['sample.ini']
config.sections()
# ['Database']
# print(config['dev'][''])
spark = SparkSession \
     .builder \
     .appName("Python Spark SQL basic example") \
     .config("spark.jars", "/home/fission/.ivy2/jars/mysql_mysql-connector-java-8.0.12.jar") \
     .getOrCreate()

dbURL="jdbc:mysql://"+ config['dev']['host'] +":"+config['dev']['port']+"/retail_db?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
print(dbURL)
df = spark.read.format("jdbc") \
     .options(url=dbURL,
              database=config['dev']['database'],
              dbtable='orders',
              user=config['dev']['user_name'],
              password=config['dev']['password'],
              driver='com.mysql.jdbc.Driver') \
     .load();
ct=df.groupBy(["order_date","order_status"]).count()
op=ct.repartition(1).write.json(path="file:///home/fission/Desktop/jsonjson")












# with open('person.json', 'w') as f:  # writing JSON object
#    json.dump(op, f)
...


# open('person.json', 'r').read()   # reading JSON object as string
# '{"hasMortgage": null, "isAlive": true, "age": 27, "address": {"state": "NY", "streetAddress": "21 2nd Street", "city": "New York", "postalCode": "10021-3100"}, "first_name": "John"}'
#

# type(open('person.json', 'r').read())



## ct.repartition(1).write.json(path="file:///home/fission/Desktop/jsonjson")

# jsonf=ct.show()
# for i in ct:
#     print(i)
# with open('jsonfile.json', 'w') as fp:
#     fp.write(json.dumps(jsonf))

 # f =open("jsonf.json","w")
# f.write(str(jsonf))
# f.close()
