package com.skk.training
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import scala.collection.mutable.ArrayBuffer
import com.twitter.chill.KryoSerializer
import org.apache.spark.storage.StorageLevel

class Stock(val name: String, val exchange: String, val series: String,
            val price: Double, val open: Double, val close: Double) extends Serializable {
  override def toString = name + ", " + exchange + ", " + series + ", " +
    price + ", " + open + ", " + close
}

object KryoSerializationExample extends App {
  val sparkConf = new SparkConf
  val useKryo = false
  if (useKryo) {
    sparkConf.setMaster("local[*]").set(
      "spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrationRequired", "true")
    //  val kryo = new KryoSerializer(sparkConf)

    sparkConf.registerKryoClasses(Array(
      classOf[com.skk.training.Stock],
      classOf[Array[com.skk.training.Stock]],
      classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
  } else {
    sparkConf.setMaster("local[*]")
  }
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.getAll.foreach(println)
  spark.sparkContext.setLogLevel("ERROR")
  val listOfStocks = List(
    new Stock("google", "NASDAQ", "EQ", 674.3, 670.6, 690.3),
    new Stock("infy", "NSE", "EQ", 9.5, 9.7, 9.3),
    new Stock("saxobank", "LSE", "EQ", 23.3, 21.2, 28.2),
    new Stock("statebank", "BSE", "EQ", 3.1, 3.4, 3.3))
  val replicatedListOfStocks = List.fill(10)(listOfStocks).flatten
  val stocksRDD = spark.sparkContext.parallelize(replicatedListOfStocks)
  println("No of partitions of stocksRDD: " + stocksRDD.getNumPartitions)
  stocksRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  println(stocksRDD.map(_.price).reduce(_ + _))
  println("executor memory status: " + spark.sparkContext.getExecutorMemoryStatus)
  println("memory used for storage: " + spark.sparkContext.getExecutorStorageStatus(0).memUsed)

}
