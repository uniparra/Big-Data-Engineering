from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

'''Funcionamiento de la función Row(), observar que funciona como una lista indexada '''
def funcRow():
    # Creamos la fila
    blogRow= Row(6,"Reynold","Xin", "https://tinyurl.6",255568,"3/2/2015",["twitter","LinkedIn"])

    #Accedemos usando la indexación
    print(blogRow(1))

'''Creamos el dataFrame. Obesrvar la salida. Se usará en el tercerExp().'''
def crearDFmedianteSchema(fireDF):
    '''Se define un schema para optimizar la lectura de los datos,
    ya que se trata de un dataset con 4 Millones de entradas'''
    spark = (SparkSession.builder.appName("RenameAddDropCol").getOrCreate())
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                              StructField('UnitID', StringType(), True),
                              StructField('IncidentNumber', IntegerType(), True),
                              StructField('CallType', StringType(), True),
                              StructField('CallDate', StringType(), True),
                              StructField('WatchDate', StringType(), True),
                              StructField('CallFinalDisposition', StringType(), True),
                              StructField('AvailableDtTm', StringType(), True),
                              StructField('Address', StringType(), True),
                              StructField('City', StringType(), True),
                              StructField('Zipcode', IntegerType(), True),
                              StructField('Battalion', StringType(), True),
                              StructField('StationArea', StringType(), True),
                              StructField('Box', StringType(), True),
                              StructField('OriginalPriority', StringType(), True),
                              StructField('Priority', StringType(), True),
                              StructField('FinalPriority', IntegerType(), True),
                              StructField('ALSUnit', BooleanType(), True),
                              StructField('CallTypeGroup', StringType(), True),
                              StructField('NumAlarms', IntegerType(), True),
                              StructField('UnitType', StringType(), True),
                              StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                              StructField('FirePreventionDistrict', StringType(), True),
                              StructField('SupervisorDistrict', StringType(), True),
                              StructField('Neighborhood', StringType(), True),
                              StructField('Location', StringType(), True),
                              StructField('RowID', StringType(), True),
                              StructField('Delay', FloatType(), True)])
    fire_df = spark.read.csv(fireDF,header=True, schema=fire_schema)
    return fire_df

'''Guardamos el dataframe como Parquet de distintas formas.'''
def saveParquet(fireDF: DataFrame):
   # Guardamos como un fichero parquet, mediante save().
    fireDF.write.mode("overwrite").format("parquet").save("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\sf-fire-calls.parquet")

   # Guardamos como un fichero parquet, mediante parquet().
    fireDF.write.mode("overwrite").parquet("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\(1)sf-fire-calls.parquet")
   #Guardamos como una tabla.
    parquetTabla = "(2)sf-fire-calls"
    '''Cuarentena. Un error desconocido. Volver despues de ver la funcion
      saveAsTable() más a fondo'''
    # fireDF.write.mode(SaveMode.Overwrite).option("path","C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA").format("parquet").saveAsTable("(2)sf-fire-calls")

'''Cuarentena. Un error desconocido. Volver despues de ver la funcion
      saveAsTable() más a fondo'''
    # fireDF.write.mode(SaveMode.Overwrite).option("path","C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA").format("parquet").saveAsTable("(2)sf-fire-calls")

'''Filtrados'''
def filtrado(fireDF):

    few_fire_df = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType").where(col("CallType") != "Medical Incident")

    few_fire_df2 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")\
        .where(col("CallType") != "Medical Incident").where(col("IncidentNumber") != 2003235)

    few_fire_df3 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")\
        .where((col("CallType") == "Medical Incident") & (col("IncidentNumber")!= 2003235) & (col("AvailableDtTm").isNotNull()))

    few_fire_df4 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")\
        .where((col("CallType") == "Medical Incident") & (fireDF.IncidentNumber != 2003235))

    few_fire_df5 = fireDF.select("IncidentNumber", "AvailableDtTm", "CallType")\
        .filter(fireDF.CallType == "Medical Incident")\
        .where(fireDF.IncidentNumber != 2003235)\
        .where(fireDF.IncidentNumber != 2003242)

    print("_____________________.where() sencillo_________________")
    few_fire_df.show(20,truncate = False)
    print("_____________________.where() con &&(=!=)_________________")
    few_fire_df2.show(20,truncate = False)
    print("_____________________.where() con &&(===) y .isNotNull()_________________")
    few_fire_df3.show(20,truncate = False)
    print("_____________________.where() con fireDF('nombreCol')_________________")
    few_fire_df4.show(20,False)
    print("_____________________.where() con filter y where. Distinta forma concatenar distintos filtrados_________________")
    few_fire_df5.show(20,False)
    print("_____________________.where() con Agregación_________________") #...Observar que es distinto que en SCALA donde se usa la funcion 'as'
    fireDF.select("CallType").where(col("CallType").isNotNull()).agg(countDistinct("CallType").alias("DistinctCallTypes")).show()

def modificarNombreCol(fireDF):
    newFireDF = fireDF.withColumnRenamed("Delay","ResponseDelayedinMins")
    newFireDF.select("IncidentNumber", "AvailableDtTm", "CallType", "ResponseDelayedinMins").show(20)
    return newFireDF

'''to_timestamp(), to_date(), drop() y withColumn() '''
def conversionFechas(fireDF):
    # withColumn() sobreescribe la columna en el nuevo DataFrame, o agrega otra columna. En este caso crea uno nuvo y luego borramos la antigua.
    fireTsDF = fireDF.withColumn("IncidentDate", to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")\
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"),"MM/dd/yyyy")).drop("WatchDate")\
        .withColumn("AvailableDtTS", to_date(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

    fireTsDF.select("IncidentDate", "OnWatchDate","AvailableDtTS").show(5,False)
def agregaciones(fireDF, fireTsDF):
    print("groupBy(), orderBy()")
    fireDF.select("CallType").where(col("CallType").isNotNull())\
        .groupBy("CallType").count()\
        .orderBy(desc("count"))\
        .show(10,False)
    print("......... funciones de agregación sum(), min(),max(),avg()............")
    #Observar la definición de F en los import.
    fireTsDF.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),F.min("ResponseDelayedinMins"),F.max("ResponseDelayedinMins"))\
        .show()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("__________________________________Función Row()__________________________________")
    funcRow()
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Creación DF mediante un schema__________________________________")
    df = crearDFmedianteSchema("C:/Users/unai.iparraguirre/Documents/BIG DATA/LearningSparkV2-master/chapter3/data/sf-fire-calls.csv")
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Escritura en distintos tipos de ficheros__________________________________")
    saveParquet(df)
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Filtrados__________________________________")
    filtrado(df)
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Modificar DF__________________________________")
    df1 = modificarNombreCol(df)
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Conversión de fechas__________________________________")
    conversionFechas(df)
    print("____________________________________________________________________")
    print(" ")
    print("__________________________________Agregaciones__________________________________")
    agregaciones(df, df1)
    print("_________________________________________________FIN_____________________________________________________")

