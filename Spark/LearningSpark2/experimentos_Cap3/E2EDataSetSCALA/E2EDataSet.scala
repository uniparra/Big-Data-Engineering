package ocb.kalamu

import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{asc, col, desc}
import org.apache.spark.sql.{functions => F}


/**
 * @author ${user.name}
 */
case class DeviceIoTData (device_id: Long, ip: String,cca2: String, device_name: String,
                          cca3: String, cn: String,
                          latitude: Double, longitude: Double,
                          scale:String, temp: Long, humidity: Long,
                          battery_level: Long,
                          c02_level: Long, lcd: String,
                          timestamp: Long)
object E2EDataSet {
  def main(args : Array[String]) {
    val spark=SparkSession.builder().appName("Nombre").getOrCreate()
    import spark.implicits._

    val ds = spark.read.json("C:\\Users\\unai.iparraguirre\\Documents\\BIG DATA\\LearningSparkV2-master\\databricks-datasets\\learning-spark-v2\\iot-devices\\iot_devices.json")
      .as[DeviceIoTData]
    primerEj(ds)
    segundoEj(ds)
    tercerEj(ds)
    cuartoEj(ds)
    spark.close()
  }
  /**Detectar dispositivos defectuosos con niveles de batería por debajo de un umbral(el que sea)**/
  def primerEj(IoTDS:Dataset[DeviceIoTData]): Unit ={
    //Observar el uso de funcion implícita
    IoTDS.filter(d => {d.battery_level < 3}).show()

    /** Observamos que hay una columna llamada longitud y que contiene valores negativos. Ya que esto no es algo normal podemos asumir que son estos aparatos los que están rotos
     * añadimos a la consulta anterior lo observado ahora */
    IoTDS.filter(d => {d.battery_level < 3}).filter{d => {d.longitude < 0}}.show()
  }

  def segundoEj(IoTDS:Dataset[DeviceIoTData]): Unit ={
    //Estudiamos primero la situaciíon del c02 en un país cualquiera
      //IoTDS.filter(d => {d.cn == "Germany"}).select()
    //Intentamos inferir la unidad del nivel c02, mediante el orden de magnitud de la suma de c02_level en dicho país
      //IoTDS.filter(d => {d.cn == "Germany"}).select(F.sum("c02_level")).show()
      //print(IoTDS.filter(d=>{d.cn =="Germany"}).count())
    /**
    +--------------+
    |sum(c02_level)|
    +--------------+
    |       9526623|
    +--------------+
     lo cual no nos dice nada; ya que queda fuera de los indices aplicados normalmente a la medicion del c02.

     Es por eso que tomaremos el valor de co2 Aleman de baremo, es decir: 9526623*/

    /**1ª Forma: Si suponemos que lo que determina el alto nivel de emisiones es la columna c02_level", entonces la consulta debe ser**/
    IoTDS
      .groupBy("cn")// se agrupa por estados
      .agg(F.sum("c02_level") as "sumaCO2")//se agrega una columna donde se suman los c02_level de cada estado
      .orderBy(F.desc("sumaCO2"))//se ordena de mayor a menor total de c02_level
      .show()

    /**2ª Forma: Si suponemos que lo que determina el alto nivel de emisiones es la columna lcd = "red", entonces la consulta debe ser**/
    IoTDS.filter(d => {d.lcd == "red"})//Se filtran solo aquellos que tengan la columna lcd = red
      .groupBy("cn")//Se agrupa por estado
      .count()//Se cuenta el numero de ocurrencia de cada estado
      .orderBy(desc("count"))//Se ordena de mayor a menor ocurrencia
      .show()
  }

  def tercerEj(IoTDS:Dataset[DeviceIoTData]): Unit ={
    IoTDS.select(F.max(col("temp")),F.max(col("c02_level")), F.max(col("battery_level")), F.max(col("humidity"))).show()
    //Moraleja: no se puede llamar a la misma columna dos veces, ni siquiera si lo que queremos es mostrar varias funciones sobre ella.
    // es decir no se puede pones un max(1col) junto con un min(1col)
    IoTDS.select(F.min(col("temp")),F.min(col("c02_level")), F.min(col("battery_level")), F.min(col("humidity"))).show()
  }

  def cuartoEj(IoTDS:Dataset[DeviceIoTData]): Unit ={
    IoTDS.filter(col("cn").isNotNull && col("temp").isNotNull).filter(d => {d.cn != ""}).groupBy("cn").agg(F.avg("temp") as "averageTemp").orderBy(asc("cn")).show()
    IoTDS.filter(col("c02_level").isNotNull && col("temp").isNotNull).groupBy("c02_level").agg(F.avg("temp") as "averageTemp").orderBy(desc("c02_level")).show()
    IoTDS.filter(col("humidity").isNotNull && col("temp").isNotNull).groupBy("humidity").agg(F.avg("temp") as "averageTemp").orderBy(desc("humidity")).show()

    /**"Sintetizamos" las tres consultas en una. Remarcar que no es exactamente lo mismo, ya que la agregación queda determinada por la agrupación. **/
    //Sintetizamos las consultas anteriores en una.
    IoTDS.filter(col("humidity").isNotNull && col("temp").isNotNull && col("c02_level").isNotNull).filter(d => {d.cn != ""})
      .groupBy("cn").agg((F.avg("temp") as "TempMedia"), (F.sum("c02_level") as "sumC02_level"), (F.avg("humidity") as "humedadMedia"))
      .orderBy(col("cn").asc, col("TempMedia").desc, col("sumC02_level").desc,col("humedadMedia").desc)
      .show()
  }
}
