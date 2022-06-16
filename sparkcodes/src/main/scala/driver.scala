
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import publicis_sapient1.function1
import publicis_sapient2.function2
import luxoft.luxsoft_function
import impetus.impetus_function
object Test {
  def main(args:Array[String]){
    val spark=SparkSession.builder.appName("test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
      import spark.implicits._
      function1(spark)
     function2(spark)
        luxsoft_function(spark)
        impetus_function(spark)
      
    

   
   
   
   
    
   
  }
  
    
}