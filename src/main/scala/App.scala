import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode
import com.databricks.spark.avro._

object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local")
    //System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read.format("json").load("src/main/resources/")
    df.registerTempTable("df")
    val purchase = df.select($"id", $"firstName", $"lastName",  explode($"purchase")).as("purchase")
    System.out.println(Console.BLUE)
    df.printSchema()
    System.out.println("Liczba zakupów ", df.count())

    val betterPurchase = purchase.select($"id", $"firstName", $"lastName", $"col.name", $"col.price" )
    betterPurchase.filter(betterPurchase("price")> 20).map(p => "First name: " + p(1) + " last name: " + p(2) + "name of product: " + p.getAs[String]("name") + " price: "+ p.getAs[String]("price")).collect().foreach(println)
    betterPurchase.filter(betterPurchase("price")> 20).map(p => "Product: " + p.getAs[String]("name") + " price: "+ p.getAs[String]("price")).distinct().collect().foreach(println)
    betterPurchase.groupBy("id").sum("price").show()

    betterPurchase.write.avro("src/main/resources/output/avro")
    betterPurchase.write.json("src/main/resources/output/json")
  }
}
