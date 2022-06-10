package ocb.kalamu
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
/**
 * @author ${user.name}
 */
object RowSCALA {
  def main(args : Array[String]) {
    println("__________________________________Función Row()__________________________________")
    funcRow()
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Creación DF mediante un schema__________________________________")
    val df = crearDFmedianteSchema()
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Escritura en distintos tipos de ficheros__________________________________")
    saveParquet(df)
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Filtrados__________________________________")
    filtrado(df)
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Modificar DF__________________________________")
    val df1 = modificarNombreCol(df)
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Conversión de fechas__________________________________")
    conversionFechas(df)
    println("____________________________________________________________________")
    println(" ")
    println("__________________________________Agregaciones__________________________________")
    agregaciones(df, df1)
    println("_________________________________________________FIN_____________________________________________________")
  }

  /** Funcionamiento de la función Row(), observar que funciona como una lista indexada **/
  def funcRow(): Unit ={
    // Creamos la fila
    val blogRow= Row(6,"Reynold","Xin", "https://tinyurl.6",255568,"3/2/2015",Array("twitter","LinkedIn"))

    //Accedemos usando la indexación
    println(blogRow(1))
  }

  /** Creamos el dataFrame. Obesrvar la salida. Se usará en el tercerExp().**/
  def crearDFmedianteSchema(): DataFrame ={
    /** Se define un schema para optimizar la lectura de los datos,
     * ya que se trata de un dataset con 4Millones de entradas **/
    val fire_schema = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitID", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)))

    val sf_fire_file = "C:/Users/unai.iparraguirre/Documents/BIG DATA/LearningSparkV2-master/chapter3/data/sf-fire-calls.csv"
    val spark = SparkSession.builder.appName("Row").getOrCreate()
    val fireDF = spark.read.schema(fire_schema).option("header",true).csv(sf_fire_file)
    //fireDF.show(50, truncate = true)
    //fireDF.show(50, truncate = false)
    return fireDF
  }

  /** Guardamos el dataframe como Parquet de distintas formas. **/
  def saveParquet(fireDF: DataFrame): Unit ={
    // Guardamos como un fichero parquet, mediante save().
    fireDF.write.mode(SaveMode.Overwrite).format("parquet").save("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\sf-fire-calls.parquet")

    // Guardamos como un fichero parquet, mediante parquet().
    fireDF.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\(1)sf-fire-calls.parquet")
    // Guardamos como una tabla.
    val parquetTabla = "(2)sf-fire-calls"

    /** Cuarentena. Un error desconocido. Volver despues de ver la funcion
     * saveAsTable() más a fondo */
    // fireDF.write.mode(SaveMode.Overwrite).option("path","C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA").format("parquet").saveAsTable("(2)sf-fire-calls")
  }

  /***Filtrados***/
  def filtrado(fireDF: DataFrame): Unit ={

    val few_fire_df = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    val few_fire_df2 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident" && col("IncidentNumber") =!= 2003235)

    val few_fire_df3 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") === "Medical Incident" && col("IncidentNumber")=!= 2003235 && col("AvailableDtTm").isNotNull)

    val few_fire_df4 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(fireDF("CallType") === "Medical Incident" && fireDF("IncidentNumber") =!= 2003235)

    val few_fire_df5 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")
      .filter(fireDF("CallType") === "Medical Incident")
      .where(fireDF("IncidentNumber") =!= 2003235)
      .where(fireDF("IncidentNumber") =!= 2003242)

    print("_____________________.where() sencillo_________________")
    few_fire_df.show(20,truncate = false)
    print("_____________________.where() con &&(=!=)_________________")
    few_fire_df2.show(20,truncate = false)
    print("_____________________.where() con &&(===) y .isNotNull()_________________")
    few_fire_df3.show(20,truncate = false)
    print("_____________________.where() con fireDF('nombreCol')_________________")
    few_fire_df4.show(20,false)
    print("_____________________.where() con filter y where. Distinta forma concatenar distintos filtrados_________________")
    few_fire_df5.show(20,false)
    print("_____________________.where() con Agregación_________________")
    fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct("CallType") as "DistinctCallTypes").show()
  }
  def modificarNombreCol(fireDF:DataFrame): DataFrame ={
    val newFireDF = fireDF.withColumnRenamed("Delay","ResponseDelayedinMins")
    newFireDF.select("IncidentNumber", "AvailableDtTm", "CallType", "ResponseDelayedinMins"/**,"Delay" da error. Es decir sobreescrito el nombre de la col**/).show(20)
    return newFireDF
}

  /** to_timestamp(), to_date(), drop() y withColumn() **/
  def conversionFechas(fireDF:DataFrame): Unit ={
    val fireTsDF = fireDF.withColumn("IncidentDate", to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")
      // withColumn() sobreescribe la columna en el nuevo DataFrame, o agrega otra columna. En este caso crea uno nuvo y luego borramos la antigua.
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"),"MM/dd/yyyy")).drop("WatchDate")
      .withColumn("AvailableDtTS", to_date(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

    fireTsDF.select("IncidentDate", "OnWatchDate","AvailableDtTS").show(5,false)
  }

  /**Funciones de agregación típicas**/
  def agregaciones(fireDF:DataFrame, fireTsDF:DataFrame): Unit ={
    print("groupBy(), orderBy()")
    fireDF.select("CallType").where(col("CallType").isNotNull)
      .groupBy("CallType").count()
      .orderBy(desc("count"))
      .show(10,false)
    print("......... funciones de agregación sum(), min(),max(),avg()............")
    //Observar la definición de F en los import.
    fireTsDF.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),F.min("ResponseDelayedinMins"),F.max("ResponseDelayedinMins"))
      .show()
  }
}
