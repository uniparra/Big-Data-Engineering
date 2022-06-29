package ocb.kalamu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr

/**
 * @author ${user.name}
 */
object Operations {

  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)
  
  def main(args : Array[String]) {
    val spark = SparkSession.builder.appName("Operaciones").getOrCreate()
    import spark.implicits._
    val delaysPath = "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\databricks-datasets\\learning-spark-v2\\flights\\departuredelays.csv"
    val airportsPath = "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\databricks-datasets\\learning-spark-v2\\flights\\airport-codes-na.txt"

    val airports = spark.read.option("header", "true")
      .option("inferSchema","true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")

    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    val foo = delays.where(expr("""origin == "SEA" AND destination =="SFO" AND date LIKE "01010%" AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    /**Vemos los DF creados**/
    println("airports-na")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    println("Delays")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    println("foo")
    spark.sql("SELECT * FROM foo").show()

    /**Operaciones**/
    println("-----------------------------OPERACIONES---------------------------")
    /**UNION**/
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    println("operacion UNION")
    bar.show(10)
    bar.filter(expr("""origin == "SEA" AND destination == "SFO" AND date LIKE "01010%" AND delay > 0 """)).show()
    spark.sql("""SELECT * FROM bar WHERE origin ="SEA" AND destination =="SFO" AND date LIKE "01010%" AND delay > 0 """).show()

    /**JOINS**/
    println("Operación JOIN")
    foo.join(airports.as("air"), $"air.IATA" === $"origin")
      .select("City", "State", "date","delay","distance","destination").show()
    spark.sql(
      """SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination FROM foo f JOIN airports_na a ON a.IATA = f.origin""").show()

    /**WINDOWING**/
    println("Preparacion de datos para Windowing")
    spark.sql("""DROP VIEW IF EXISTS departureDelaysWindow""")
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW departureDelaysWindow AS SELECT origin, destination, SUM(delay) AS TotalDelays FROM departureDelays WHERE origin IN ("SEA","SFO","JFK") AND destination IN ("SEA","SFO","JFK","DEN","ORD","LAX","ATL") GROUP BY origin, destination""")
    spark.sql("""SELECT * FROM departureDelaysWindow""").show()
    println("Operación WINDOWING")
    spark.sql(
      """ SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
        |FROM departureDelaysWindow WHERE origin IN ("JFK","SEA","SFO") GROUP BY origin, destination ORDER BY SUM(TotalDelays) DESC LIMIT 10 """.stripMargin).show()

    println("-- dense_rank() ---> Esta función devuelve el rango de cada fila dentro de una partición del conjunto de resultados, sin espacios en los valores de clasificación.\n El rango de una fila específica es uno más el número de valores de rango distintos anteriores a esa fila específica.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 3 """).show()
    println("-- percent_rank() ---> Calcula el rango relativo de una fila dentro de un grupo de filas")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, percent_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 3 """).show()
    println("-- ntile(int) ---> Distribuye las filas de una partición ordenada en un número especificado de grupos. Los grupos se numeran a partir del uno.\n Para cada fila, NTILE devuelve el número del grupo al que pertenece la fila.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, ntile(6) OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 6 """).show()
    println("-- row_number() ---> Enumera los resultados de un conjunto de resultados. Concretamente, devuelve el número secuencial de una fila dentro de una\n partición de un conjunto de resultados, empezando por 1 para la primera fila de cada partición.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, row_number() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 5 """).show()
    println("-- cume_dist() ---> esta función calcula la distribución acumulativa de un valor en un grupo de valores. Es decir, CUME_DIST calcula la posición relativa de un valor especificado en un grupo de valores.\n Suponiendo un orden ascendente, el CUME_DIST de un valor en la fila r se define como el número de filas\n con valores menores o iguales que el valor de la fila r, dividido entre el número de filas evaluadas en la partición o el conjunto de resultados de la consulta. CUME_DIST es similar a la función PERCENT_RANK.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, cume_dist() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t WHERE rank <= 3 """).show()
    println("-- first_value() ---> Devuelve el primer valor de un conjunto ordenado de valores de cada partición.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, first_value(TotalDelays) OVER (ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t  """).show()
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, first_value(TotalDelays) OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t """).show()
    println("-- last_value() ---> Devuelve el último valor de un conjunto ordenado de valores de cada particion.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, last_value(TotalDelays) OVER (PARTITION BY origin ORDER BY TotalDelays ASC) as rank FROM departureDelaysWindow) t """).show()
    println("-- lag() --->  LAG proporciona acceso a una fila en un desplazamiento físico especificado que hay antes de la fila actual.\n Use esta función analítica en una instrucción SELECT para comparar valores de la fila actual con valores de una fila anterior.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, lag(TotalDelays,1,0) OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t """).show()
    println("-- lead() ---> LEAD proporciona acceso a una fila en un desplazamiento físico especificado que hay después de la fila actual. Use esta función analítica en una instrucción SELECT para comparar valores de la fila actual con valores de una fila posterior.")
    spark.sql("""SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, lead(TotalDelays,1,0) OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank FROM departureDelaysWindow) t  """).show()

    /**MODIFICATIONS**/
    println("-------------------MODIFICACIONES----------------------------------------------------")
    /**NewCol*/
    println("DF sobre el que se haran modificaciones")
    foo.show()
    println("Nueva columna")
    val foo2 = foo.withColumn("status", expr("""CASE WHEN delay <= 10 THEN "On-time" ELSE "Delayed" END  """))
    foo2.show(false)

    /**DropCol*/
      println("Eliminar columna")
    val foo3 = foo2.drop("delay")
    foo3.show(false)

    /** RenameCol */
      println("Renombrar columna")
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show(false)

    /** Pivoting */
    println("Pivotaje o cambio de orden de columnas")
    spark.sql(
      """ SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
        |FROM departureDelays
        |WHERE origin = 'SEA' """.stripMargin).show(false)
    println("En pivotaje también permite poner nombres a columnas así como realizar cálculos agregados")
    spark.sql(""" SELECT * FROM (
                |SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                |FROM departureDelays WHERE origin = 'SEA'
                |)
                |PIVOT (
                |CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
                |FOR month IN (1 JAN, 2 FEB)
                |)
                |ORDER BY destination """.stripMargin)

  }

}
