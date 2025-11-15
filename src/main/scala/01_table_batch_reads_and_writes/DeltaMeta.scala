// src/main/scala/DeltaMetaExample.scala
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._
import java.io.File

object DeltaMetaExample {
  def main(args: Array[String]): Unit = {

    // Initialize Spark session with Delta Lake extensions
    val spark = SparkSession.builder()
      .appName("DeltaMetaExample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    println(s"Spark Version: ${spark.version}")
    import spark.implicits._

    val deltaPath = "/tmp/delta/people10m"
    val tableName = "default.people10m"

    // Create or replace managed metastore table (idempotent)
    println(s"\nCreating/Replacing Managed Delta table: $tableName")
    DeltaTable.createOrReplace(spark)
      .tableName(tableName)
      .addColumn("id", "INT")
      .addColumn("firstName", "STRING")
      .addColumn("middleName", "STRING")
      .addColumn("lastName", "STRING")
      .addColumn("gender", "STRING")
      .addColumn("birthDate", "TIMESTAMP")
      .addColumn("ssn", "STRING")
      .addColumn("salary", "INT")
      .location(deltaPath)
      .execute()
    println(s"Managed table $tableName creation/check complete.")

    // Verify path-based Delta table exists
    println(s"\nChecking Delta table at path: $deltaPath")
    if (DeltaTable.isDeltaTable(spark, deltaPath)) {
      println(s"Delta table at $deltaPath exists and schema is verified.")
    } else {
      println(s"Path $deltaPath is not recognized as a Delta table!")
    }

    // Create sample data and write to Delta table (idempotent overwrite)
    val sampleData = Seq(
      (1, "John", "A.", "Doe", "M", "1980-01-01 00:00:00", "123-45-6789", 100000),
      (2, "Jane", "B.", "Smith", "F", "1990-02-15 00:00:00", "987-65-4321", 120000)
    )

    val df = sampleData.toDF("id", "firstName", "middleName", "lastName", "gender", "birthDate", "ssn", "salary")
      .withColumn("birthDate", $"birthDate".cast("timestamp"))

    df.write.format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(deltaPath)
    println("\nSample data written (overwritten) to Delta table.")

    // Explore files and directory structure of the Delta table
    val pathDir = new File(deltaPath)

    println(s"\nFiles in $deltaPath:")
    spark.read.format("delta").load(deltaPath).show(false)

    println("\nDirectory structure:")
    if (pathDir.exists() && pathDir.isDirectory) {
      pathDir.listFiles().foreach(f => println(f.getName))
    } else {
      println(s"Directory $deltaPath does not exist.")
    }

    println("\nDelta Log:")
    val logDir = new File(s"$deltaPath/_delta_log")
    if (logDir.exists() && logDir.isDirectory) logDir.listFiles().foreach(f => println(f.getName))

    // Show Delta table history
    println("\nDelta table history (Path-based):")
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    deltaTable.history().show(false)

    println("\nDelta table history (Managed):")
    DeltaTable.forName(spark, tableName).history().show(false)

    spark.stop()
  }
}

