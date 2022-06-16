import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object random_spark_ques {
  
  def spark_random(spark:SparkSession){
    
    val list=List(("X", 5, 3, 7),("Y", 3, 3, 6),("Z", 5, 2, 6))
    import spark.implicits._
    val df=list.toDF("A","B","C","D")
    val df_new=df.withColumn("newcol", col("B")+col("C")+col("D"))
    df_new.show()
    
    def sum_cols(x:Int,y:Int,z:Int):Int = {
      
      x+y+z
    }
    
    val sum_inline=(x:Int,y:Int,z:Int)=>x+y+z
    val sum_udf=udf(sum_cols(_,_,_))
    val df_with_udf=df.withColumn("newcol_udf",sum_udf(col("B"),col("C"),col("D")))
    df_with_udf.show()
    
    val df_with_udf_inline_function=df.withColumn("newcol_udf_inline",sum_udf(col("B"),col("C"),col("D")))
    df_with_udf_inline_function.show()
    
    
    
    
    
    
  }
}