
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object publicis_sapient1 {
  
  def function1(spark:SparkSession) {
    import spark.implicits._
    val lst=List((1,"Name:Prashant;salary:1000;role:DE"),(2,"Name:Shrishti;age:27;org:facebook;city:bangalore"))
    val cols=List("id","value")
    val rdd=spark.sparkContext.parallelize(lst)
    //val df=spark.createDataFrame(rdd,cols)
    val df=lst.toDF("id","values")
    df.show()
    val df1=df.map(x=>{
      var lst=x.getString(1).split(";")
      (x.getInt(0),lst)})
    val df2=df1.toDF("id","values")
    df2.show(2)
    val df3=df2.select($"id",explode($"values"))
    df3.show()
    var lst2=Array[String]()
 
    val df4=df.map(x=>{
      var map=scala.collection.mutable.Map[String,String]()
      var lst1=x.getString(1).split(";")
      val lst3=lst1.map(y=>{
       var lst4= y.split(":")
      map+=(lst4(0)->lst4(1))
        
      })
      (x.getInt(0),map)})
    df4.show(truncate=false)
    val df5=df4.toDF("id","values")
    val df6=df5.select($"id",explode($"values"))
    df6.show(false)
    

  }
}