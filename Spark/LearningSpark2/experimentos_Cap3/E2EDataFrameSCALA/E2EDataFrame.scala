package ocb.kalamu

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
/**
 * Sobre la base de datos sf-fire-calls se hara una serie de consultas.
 */
object E2EDataFrame {
  def main(args : Array[String]): Unit = {
    //Empezamos por definir la SparkSession
    val spark = SparkSession.builder.appName("FireCallsSF").getOrCreate()

    //creamos el DataFrame. RECORDAR QUE EL METASTORE DEL FICHERO PARQUET GUARDA EL SCHEMA POR LO QUE NO HACE FALTA DEFINIRLO.
    val sfFireCallsDF = spark.read.option("header", true).option("inferSchema",true).parquet("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\sf-fire-calls.parquet")
    //arreglamos la fecha del CallDate, ya que en varios ejercicios tendremos que filtrar por años o meses.
    val dateFireDF =sfFireCallsDF.withColumn("IncidentDay", to_date(col("CallDate"), "MM/dd/yyyy")).drop("CallDate")
    dateFireDF.printSchema()

    primerEj(dateFireDF)
    segundoEj(dateFireDF)
    tercerEj(dateFireDF)
    cuartoEj(dateFireDF)
    quintoEj(dateFireDF)
    sextoEj(dateFireDF)
    spark.close()
  }

  /**¿Cuáles son los diferentes tipos de llamadas en 2018?**/
  def primerEj(fireDF:DataFrame): Unit ={
    val numLlamadas = fireDF.select("CallType", "IncidentDay")
      .where(year(col("IncidentDay")) === 2018 ).groupBy("CallType")
      .agg(countDistinct("CallType") as "DistinctCallTypes")

    println("The number of calls during 2018 is: ")
    numLlamadas.show(truncate = true)
  }

 /**¿En qué mes de 2018 se dio el mayor número de llamadas?**/
  def segundoEj(fireDF:DataFrame): Unit ={
    fireDF.select(col("IncidentDay")).where((year(col("IncidentDay"))===2018) && (month(col("IncidentDay"))===12)).show()

    //La consulta del ejercicio es la siguiente
    fireDF.select(col("IncidentDay")).where(year(col("IncidentDay")) === 2018)
      .groupBy(month(col("IncidentDay"))).count()
      .orderBy(desc("count"))
      .show(15)
  }
  /**¿Qué vecindario es el que más llamadas produjo durante el 2018?**/
  def tercerEj(fireDF:DataFrame): Unit ={
    fireDF.select(col("Neighborhood")).where(year(col("IncidentDay")) === 2018)
      .groupBy(col("Neighborhood")).count()
      .orderBy(desc("count"))
      .show(15)
  }

  /**¿Qué vecindario tuvo un tiempo de respuesta mayor durante el 2018?**/
  def cuartoEj(fireDF:DataFrame): Unit ={
    fireDF.select(col("Neighborhood"), col("Delay"))
      .where((year(col("IncidentDay"))=== 2018) && (col("Neighborhood").isNotNull) && (col("Delay").isNotNull))
      .groupBy(col("Neighborhood")).agg(sum(col("Delay")) as "SumaDelay").orderBy(desc("SumaDelay"))
      .show(500)
  }

  /**¿Cuál es la semana de 2018 en la que se recivieron más llamadas?**/
  def quintoEj(fireDF:DataFrame): Unit ={
    //Obtenemos un Dataframe que tenga como columna adicional, una que dice el numero de la semana del año de 2018
    val semanasFire = fireDF.withColumn("weekOfYear", weekofyear(col("IncidentDay")))
    //sobre el nuevo Dataframe ejecutamos la consulta. Que sigue teniendo la misma estructura que las anteriores
    semanasFire.select("weekOfYear").where(year(col("IncidentDay"))===2018)
      .groupBy("weekOfYear").count()
      .orderBy(desc("count"))
      .show()
  }

  /**¿Existe correlacion entre vecindario, código postal y el número de llamadas?**/
  def sextoEj(fireDF:DataFrame): Unit ={
    //Preparamos un Dataframe para poder echar la cuenta que queremos
    val countNumberCalls = fireDF.select("IncidentDay","Zipcode").groupBy("IncidentDay", "Zipcode").count()
    println("El coeficiente de correlacion entre el código postal y el número de llamadas es de " + countNumberCalls.stat.corr("Zipcode","count")+". ")
    println("Es decir no existe correlación de ningun tipo. ")
  }
}
