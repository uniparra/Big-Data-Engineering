Microsoft Windows [Versión 10.0.19044.1766]

(c) Microsoft Corporation. Todos los derechos reservados.

<span style="color:orange">spark-sql></span>C:\spark3\bin>beeline

Beeline version 2.3.7 by Apache Hive

<span style="color:orange">spark-sql>beeline></span> !connect jdbc:hive2://localhost:10000

<span style="color:blue">

Connecting to jdbc:hive2://localhost:10000

Enter username for jdbc:hive2://localhost:10000: unai.iparraguirre

Enter password for jdbc:hive2://localhost:10000:

Connected to: Spark SQL (version 3.0.3)

Driver: Hive JDBC (version 2.3.7)

Transaction isolation: TRANSACTION\_REPEATABLE\_READ

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> SHOW tables;

<span style="color:blue">

+-----------+------------+--------------+

| database  | tableName  | isTemporary  |

+-----------+------------+--------------+

| default   | people     | false        |

+-----------+------------+--------------+

1. row selected (0,998 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> SELECT \* FROM people;

<span style="color:blue">

+--------+------+

|  name  | age  |

+--------+------+

| Jesus  | 12   |

+--------+------+

1. row selected (2,302 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> INSERT INTO people VALUES ("Samantha", 12);

<span style="color:blue">

+---------+

| Result  |

+---------+

+---------+

No rows selected (1,625 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> SELECT \* FROM people;

<span style="color:blue">

+-----------+------+

|   name    | age  |

+-----------+------+

| Samantha  | 12   |

| Jesus     | 12   |

+-----------+------+

1. rows selected (0,366 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> INSERT INTO people VALUES ("Andy", 30);

<span style="color:blue">

+---------+

| Result  |

+---------+

+---------+

No rows selected (0,608 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> INSERT INTO people VALUES ("Michael", NULL);

<span style="color:blue">

+---------+

| Result  |

+---------+

+---------+

No rows selected (0,648 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000></span> SELECT \* FROM people;

<span style="color:blue">

+-----------+-------+

|   name    |  age  |

+-----------+-------+

| Samantha  | 12    |

| Michael   | NULL  |

| Jesus     | 12    |

| Andy      | 30    |

+-----------+-------+

4 rows selected (0,407 seconds)

</span>

<span style="color:orange">spark-sql>0: jdbc:hive2://localhost:10000> </span>
