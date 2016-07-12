import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
  * Created by bucio on 29.06.16.
  */

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local")
    val sc = new SparkContext(conf)
    /*var NUM_SAMPLES = 1000;
    val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
      val x = Math.random()
      val y = Math.random()
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)*/
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.format("json").load("src/main/resources/repositories.json")
    df.printSchema()

  }
}
