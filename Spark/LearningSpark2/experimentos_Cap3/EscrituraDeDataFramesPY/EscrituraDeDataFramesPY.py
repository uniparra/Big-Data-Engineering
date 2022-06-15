# This is a sample Python script.
from pyspark.sql import *
# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql.functions import col


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = SparkSession.builder.appName("EjercicioAdicional").getOrCreate()
    mnmDF = spark.read.option("header", True).option("inferSchema",True).csv("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\chapter2\\py\\src\\data\\mnm_dataset.csv")


    mnmDF.write.mode("overwrite").option("header", True).json(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmjson(sin repartition).json")
    mnmDF.write.mode("overwrite").option("header", True).csv(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv(sin repartition).csv")
    mnmDF.write.mode("overwrite").parquet(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmparquet(sin repartition).parquet")

    mnmDF.repartition(1, col("Count")).write.mode("overwrite").option("header", True).json("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmjson.json")
    mnmDF.repartition(8, col("Count")).write.mode("overwrite").option("header", True).csv("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv.csv")
    mnmDF.repartition(4, col("Count")).write.mode("overwrite").parquet("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmparquet.parquet")

    mnmDF.repartitionByRange(1, col("Count")).write.mode("overwrite").option("header", True).json(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmjson(ByRange).json")
    mnmDF.repartitionByRange(8, col("Count")).write.mode("overwrite").option("header", True).csv(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv(ByRange).csv")
    mnmDF.repartitionByRange(4, col("Count")).write.mode("overwrite").parquet(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmparquet(ByRange).parquet")

    spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv(ByRange).csv").show()
    spark.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv.csv").show()
    spark.read.option("header", "true").option("inferSchema", "true").csv(
        "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv(sin repartition).csv").show()

    '''Estudiando los distintos modos de guardar el fichero e imprimir por pantalla la lectura de estos ultimos se llega a varias conclusiones:
        1. El orden de almacenamiento no necesariamente se preserva en la lectura y creacion de un DataFrame (Pregunta: ¿existe alguna manera de preservar el orden?) 
        2. En este entorno y con la sintaxis de abajo se guarda en un solo fichero .csv o .json o .parquet
        3. Para elegir el numero de particiones tenemos los métodos repartition() y repartitionByRange().
        
            3.1. repartition(): se aplica HashPartitioner cuando se proporcionan una o más columnas y RoundRobinPartitioner cuando 
                 no se proporciona ninguna columna. Si se proporciona una o más columnas (HashPartitioner), esos valores se codificarán y se 
                 usarán para determinar el número de partición mediante el cálculo de algo como partition = hash(columns) % numberOfPartitions. 
                 Si no se proporciona ninguna columna (RoundRobinPartitioner), los datos se distribuyen uniformemente en el número especificado de particiones.
                 
            3.2. repartitionByRange(): dividirá los datos en función de un rango de valores de columna. Esto generalmente se usa para valores 
                 continuos (no discretos), como cualquier tipo de número. Tenga en cuenta que, por motivos de rendimiento, este método utiliza 
                 el muestreo para estimar los rangos. Por lo tanto, la salida puede no ser coherente, ya que el muestreo puede devolver valores
                 diferentes. El tamaño de la muestra puede ser controlado por la configuración spark.sql.execution.rangeExchange.sampleSizePerPartition.
        '''


    mnmDF.write.format("avro").mode("overwrite").save("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\mnmcsv.avro")


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
