import org.apache.spark.sql.SparkSession 

object DeltaApp {
  def main(args: Array[String]): Unit = {
    println("Starting Delta Lake Application ...") 

    // Initialize Spark Session with Delta Lake extensions 
    var spark = SparkSession 
      .builder() 
      .appName("SBTDeltaApp")
      .master("local[*]") // run locally 
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate() 

    println("Spark version: ${spark.version}")

    try {
      // Create a small DataFrame 
      val data = Seq((1, "A"), (2, "B")) 
      val df = spark.createDataFrame(data).toDF("id", "value") 

      val deltalPath = "/tmp/my_first_delta_table" 

      // Write as a Delta table 
      df.write.format("delta").mode("overwrite").save(deltalPath) 

      // Read the Delta table 
      var readDf = spark.read.format("delta").load(deltalPath) 
      readDf.show() 
    } finally {
      spark.stop()
    }
  } 
}
