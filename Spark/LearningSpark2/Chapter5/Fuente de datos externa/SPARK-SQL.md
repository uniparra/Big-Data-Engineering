﻿C:\Users\unai.iparraguirre>spark-sql

log4j:WARN No appenders could be found for logger (org.apache.hadoop.hive.conf.HiveConf).

log4j:WARN Please initialize the log4j system properly.

log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

22/06/24 09:40:28 INFO SharedState: spark.sql.warehouse.dir is not set, but hive.metastore.warehouse.dir is set. Setting spark.sql.warehouse.dir to the value of hive.metastore.warehouse.dir ('/user/hive/warehouse').

22/06/24 09:40:28 INFO SharedState: Warehouse path is '/user/hive/warehouse'.

22/06/24 09:40:28 INFO SessionState: Created HDFS directory: /tmp/hive/unai.iparraguirre/3b1b0761-33d4-4d63-856a-29b8a93cc8b9

22/06/24 09:40:28 INFO SessionState: Created local directory: C:/Windows/TEMP/unai.iparraguirre/3b1b0761-33d4-4d63-856a-29b8a93cc8b9

22/06/24 09:40:28 INFO SessionState: Created HDFS directory: /tmp/hive/unai.iparraguirre/3b1b0761-33d4-4d63-856a-29b8a93cc8b9/\_tmp\_space.db

22/06/24 09:40:28 INFO SparkContext: Running Spark version 3.0.3

22/06/24 09:40:28 INFO ResourceUtils: ==============================================================

22/06/24 09:40:28 INFO ResourceUtils: Resources for spark.driver:

22/06/24 09:40:28 INFO ResourceUtils: ==============================================================

22/06/24 09:40:28 INFO SparkContext: Submitted application: SparkSQL::192.168.79.228

Spark master: local[\*], Application Id: local-1656056431212

22/06/24 09:40:36 INFO SparkSQLCLIDriver: Spark master: local[\*], Application Id: local-1656056431212

<span style="color:orange">spark-sql> CREATE TABLE people (name STRING, age INT);</span>

Time taken: 3.048 seconds

22/06/24 09:45:57 INFO SparkSQLCLIDriver: Time taken: 3.048 seconds

<span style="color:orange">spark-sql> show tables;</span>

22/06/24 09:46:05 INFO CodeGenerator: Code generated in 209.5077 ms

<span style="color:blue">

default managed\_us\_delay\_fights\_tbl     false

default people  false

</span>

<span style="color:orange">spark-sql> INSERT INTO people VALUES ("Michael",NULL);</span>

Time taken: 2.061 seconds


<span style="color:orange">spark-sql> INSERT INTO people VALUES ("Andy",30);</span>

<span style="color:orange">spark-sql> INSERT INTO people VALUES ("Samantha",19);</span>

<span style="color:orange">spark-sql> SELECT \* FROM people;</span>

<span style="color:blue">

Samantha        19

Andy    30

Michael NULL

</span>

<span style="color:orange">spark-sql> SELECT \* FROM people WHERE age < 20;</span>

<span style="color:blue">

Samantha        19

</span>

<span style="color:orange">spark-sql> SELECT \* FROM people WHERE age IS NULL;</span>

<span style="color:blue">

Michael NULL

</span>

spark-sql>
