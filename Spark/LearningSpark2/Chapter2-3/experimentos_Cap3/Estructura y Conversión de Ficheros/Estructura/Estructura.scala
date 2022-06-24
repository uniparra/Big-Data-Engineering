package ocb.kalamu

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * En main se construira un DF programaticamente. Mientras que en la funcion axiliar usando DDL. Se leeran los daton 
 * en ambos casos desde un fichero json.
 */
object Estructura {

  def main(args : Array[String]) {

    val spark = SparkSession.builder.appName("EstructuraEsquemaProgramática").getOrCreate()

    val jsonFile = "C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\chapter3\\scala\\data\\blogs.json"

    // ------ Definimos el esquema de manera programática ------
    val schema = StructType(Array(StructField("Id",IntegerType, false),
                                  StructField("First", StringType, false),
                                  StructField("Last",StringType,false),
                                  StructField("Url",StringType,false),
                                  StructField("Published",StringType,false),
                                  StructField("Hits",IntegerType,false),
                                  StructField("Campaings",ArrayType(StringType),false)))

    val blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show(false)// Por defecto truncate = true.
    blogsDF.show()
    println(blogsDF.printSchema)
    println(blogsDF.schema)
    spark.stop()
    Session()
  }
  /*** Aquí encontramos la manera de definir un DF mediante DDL ***/
  def Session (): Unit ={
    val spark = SparkSession.builder.appName("Auxiliar").getOrCreate()
    val schema = "Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaings ARRAY<STRING>"
    val blogDF = spark.read.schema(schema).json("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\chapter3\\scala\\data\\blogs.json")
    blogDF.show()
    spark.close()
  }
}
