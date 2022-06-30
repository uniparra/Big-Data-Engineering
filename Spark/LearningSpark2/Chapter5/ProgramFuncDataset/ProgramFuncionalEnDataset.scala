package ocb.kalamu
import scala.util.Random._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.sql.Dataset
import scala.util.Random
import org.apache.log4j.{Level, Logger}

/**
 * @author ${user.name}
 */
case class Usage(uid:Int, uname:String, usage:Int)

object ProgramFuncionalEnDataset {
  val logg = Logger.getLogger("org")
  logg.setLevel(Level.WARN)

  def main(args : Array[String]) {
    val spark = SparkSession.builder.appName("Ejemplo").getOrCreate()
    import spark.implicits._

    val r = new Random(42)

    val data = for(i <- 0 to 1000) yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)

    Filter(dsUsage)
    dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

    Map(dsUsage, spark)
    dsUsage.map(u=>{computerCostUsage(u.usage)}).show(5,false)

    dsUsage.map(u => {computeUserCostUsage(u)}).show(5,false)
  }

  /**FILTER*/
  def Filter(ds:Dataset[Usage]): Unit ={
  ds.filter(d => d.usage > 900).orderBy(desc("usage")).show(5,false)
  }
  def filterWithUsage(u: Usage) = u.usage > 900 //Esto es programación funcional

  /**MAP*/
  def Map(ds:Dataset[Usage], spark:SparkSession): Unit ={
    import spark.implicits._
    ds.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .5}).show(5,false)
  }
  def computerCostUsage(usage:Int):Double={
    if(usage>750) usage * 0.15
    else usage * 0.5
  }//Esto es programación funcional

  case class UsageCost(uid:Int, uname:String, usage:Int, cost:Double)

  def computeUserCostUsage(u:Usage):UsageCost={
    val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.5
    UsageCost(u.uid, u.uname, u.usage, v)
  }
}
