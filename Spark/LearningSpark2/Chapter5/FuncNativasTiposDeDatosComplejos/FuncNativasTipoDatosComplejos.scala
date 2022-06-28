package ocb.kalamu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author ${user.name}
 */

object FuncNativasTipoDatosComplejos {
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)
  def main(args : Array[String]) {

    val spark = SparkSession.builder.appName("FuncNativas").getOrCreate()
    import spark.implicits._
    val t1 = Array(35,36,32,30,40,42,38)
    val t2 = Array(31,32,34,55,56)
    val tc = Seq(t1, t2).toDF("celsius")
    tc.createOrReplaceTempView("tC")
    tc.show(false)
    //tc.transform() parece que tiene sentido con Dataset y no con Dataframes. ESTUDIAR como funcionan las columnas con DF.
    Transform(spark)
    Filter(spark)
    Exists(spark)
   // Reduce(spark)
  }

  def Transform(spark:SparkSession): Unit ={
    spark.sql("""SELECT celsius, transform(celsius, t->((t*9) div 5) + 32) as fahrenheit FROM tc""").show(false)
  }
  def Filter(spark:SparkSession): Unit ={
    spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tc""").show(false)
  }
  def Exists(spark:SparkSession): Unit ={
    spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as threshold FROM tc""").show(false)
  }
  def Reduce(spark:SparkSession): Unit ={
    spark.sql("""SELECT celsius, reduce(tc["celsius"], 0, (t, acc) -> t + acc, acc -> (acc div size(celsius)*9 div 5) + 32) as avgFahrenheit FROM tc""").show(false)
  }
}
