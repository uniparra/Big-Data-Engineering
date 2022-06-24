package ocb.kalamu

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

/**
 * Se creará un dataframe y se guardará en formato json, parquet y csv.
 */
object App {

  def main(args : Array[String]){
    val spark =SparkSession
      .builder
      .appName("NombresEdades")
      .getOrCreate()

    // Creamos el dataframe
    val dataFM = spark
      .createDataFrame(Seq(("Brook",23),("Brooke",20),("Denny",31),("Jules",30),("TD",35))).toDF("name","age")

     /*** Se especifica que Overwrite ya que de otra manera si el directorio no existe arroja un error. ***/
    // ---- json ----
    dataFM.write.mode(SaveMode.Overwrite).json("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\ejemplo.json") //

    // ---- parquet ----
    dataFM.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\ejemplo.parquet")

    // ---- csv ----
    dataFM.write.mode(SaveMode.Overwrite).csv("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\DATA\\ejemplo.csv")
    dataFM.printSchema()
    println(dataFM.schema)
    dataFM.show()
  }

}
