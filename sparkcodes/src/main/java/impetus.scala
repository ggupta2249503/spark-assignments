import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object impetus {
  def impetus_function(spark:SparkSession){
    
     //val df_sample1=spark.read.option("delimiter", ",").text("src/main/resources/sample1.txt")
    val df_sample1_csv=spark.read.csv("src/main/resources/sample1.txt").toDF("id","word")
    //df_sample1.show()
    df_sample1_csv.show()
    val df_sample2_csv=spark.read.csv("src/main/resources/sample2.txt").toDF("id","word")
    df_sample2_csv.show()
    val df_union=df_sample1_csv.union(df_sample2_csv)
    val df_union_sel=df_union.select(col("word"))
    val  df_union_sel_grp=df_union_sel.groupBy(col("word")).count()
    df_union_sel_grp.show(false)
  }
}