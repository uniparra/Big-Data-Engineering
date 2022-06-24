from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,DataType, IntegerType, StringType,ArrayType
from pyspark.sql.functions import array

schema = "Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaings ARRAY<STRING>"

data = [[1,"Jules","Damji","http://tinyurl.1","1/4/2016",4535,["twitter","LinkedIn"]],[2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
                                                                                                                                                    "LinkedIn"]],
        [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
                                                                    "twitter", "FB", "LinkedIn"]],
        [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
         ["twitter", "FB"]],
        [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
                                                                         "twitter", "FB", "LinkedIn"]],
        [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
         ["twitter", "LinkedIn"]]
        ]
if __name__=="__main__":
    spark = SparkSession.builder.appName("Python").getOrCreate()
    blogs_df = spark.createDataFrame(data,schema)
    blogs_df.show()
    print(blogs_df.printSchema())
    spark.close()
