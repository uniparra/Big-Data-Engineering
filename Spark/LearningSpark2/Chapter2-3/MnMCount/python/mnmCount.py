import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
#en caso de hacerlo en e workspace de databrics.
#¿Es necesario importar todo esto?

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)


#Iniciamos un SparkSession.
#Importante señalar que por cada JVM puede haber un solo SparkSession
spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

#Guardamos en mnm_file el nombre del archivo del conjunto de datos que se introducira por la línea de comandos
mnm_file = sys.argv[1]

#Se lee el fichero desde el DataFrame de Spark usando CSV
#formato inferido del esquema y especificando que el archivo contiene
#un encabezado que proporciona nombres de columnas separando los campos
#por una coma
mnm_df=(spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(mnm_file))
#Notemos que no usamos RDD en aboluto.

#Notemos que ya que algunas de las funciones de Spark
#devuelven el mismo ojeto, podemos encadenar las llamadas de funciones.

#Acontinuación hacemos
#1 Seleccionar de DataFrame los campos "State", "Color", y "Count".
#2 Dado que queremos agrupar cada estado y color con el count, usamos groupBy()
#3 Sumamos los count de todos los colores y agrupamos (groupBy()) por estado y color.
#4 orderBy() en orden descendiente

count_mnm_df=(mnm_df
              .select("State","Color","Count")
              .groupBy("State","Color")
              .agg(count("Count").alias("Total"))
              .orderBy("Total",ascending=False))

#Muestre las agregaciones resultantes para todos los estados y colores; Un recuento
#total de cada color por estado. Notar que show() es una acción, que activará la consulta
#anterior, es decir se ejecutará.

count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))


#Para ver los datos por ejemplo de California
#1 seleccionamos todas las filas del DataFrame
#2 Filtramos solo el estado de California
#3 groupBy() estado y color como lo hicimos arriba
#4 Agregamos los conteos para cada color
#5 orderBy() en orden descendente

ca_count_mnm_df = (mnm_df
                 .select("State","Color","Count")
                 .where(mnm_df.State =="CA")
                 .groupBy("State","Color")
                 .agg(count("Count").alias("Total"))
                 .orderBy("Total",ascending=False))

#Mostramos los resultados
ca_count_mnm_df.show(n=10, truncate=False)

#Paramos el SparkSession
spark.stop()
