
import org.apache.spark.sql.SparkSession

object dfusingcsv {
  def main(args:Array[String]){
    val spark= SparkSession.builder()
                          .master("local[1]")
                          .appName("sparkdistinct")
                          .getOrCreate()
                          
    // import spark.implicits._                     
  val df = spark.read.csv("C:\\csv\\EMP_Sapient.csv")
  df.show(false)
}
}