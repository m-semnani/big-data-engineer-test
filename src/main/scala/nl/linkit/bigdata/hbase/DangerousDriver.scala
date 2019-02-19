package nl.linkit.bigdata.hbase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DangerousDriver {

  val catalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"dangerous-driver", "tableCoder":"PrimitiveType"},
       |"rowkey":"id",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"id", "type":"int"},
       |"eventId":{"cf":"f", "col":"eventId", "type":"int"},
       |"driverId":{"cf":"f", "col":"driverId", "type":"int"},
       |"driverName":{"cf":"f", "col":"driverName", "type":"string"},
       |"eventType":{"cf":"f", "col":"eventType", "type":"string"},
       |"latitudeColumn":{"cf":"f", "col":"latitudeColumn", "type":"float"},
       |"longitudeColumn":{"cf":"f", "col":"longitudeColumn", "type":"float"},
       |"routeId":{"cf":"cf1", "col":"routeId", "type":"int"},
       |"routeName":{"cf":"cf1", "col":"routeName", "type":"string"},
       |"truckId":{"cf":"cf1", "col":"truckId", "type":"int"}
       |}
       |}""".stripMargin

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataCoderExample")
      .master("local[1]")
      .getOrCreate()

    handleDriverTimeSheet(spark)

    handleDangerousDriver(spark)

  }

  def handleDriverTimeSheet(spark: SparkSession): Unit = {
    // Read first csv
    val driverFrame = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/home/mohammad/IdeaProjects/linkit/src/main/resources/data-spark/drivers.csv")

    // Read second csv
    val timesheetFrame = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/home/mohammad/IdeaProjects/linkit/src/main/resources/data-spark/timesheet.csv")

    // Join csv
    var joinedFrame = driverFrame.join(timesheetFrame, Seq("driverId"))
    joinedFrame = joinedFrame.withColumnRenamed("hours-logged", "hours")
      .withColumnRenamed("miles-logged", "miles")

    joinedFrame.createOrReplaceTempView("timesheet")

    // Generate result
    spark.sql("select driverId, first(name), sum(hours), sum(miles) from timesheet group by driverId ")
      .show(100)

  }

  def handleDangerousDriver(spark: SparkSession): Unit = {
    // Read first csv
    val dangerousFrame = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/home/mohammad/IdeaProjects/linkit/src/main/resources/data-hbase/dangerous-driver.csv")

    // Read second csv
    val extraFrame = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("/home/mohammad/IdeaProjects/linkit/src/main/resources/data-hbase/extra-driver.csv")

    // Uniform two data frames
    var unionFrame = dangerousFrame.union(extraFrame)
    //TODO figure out event time problem while saving data to HBase
    unionFrame = unionFrame.drop(unionFrame.col("eventTime"))

    // add id column to data frame
    unionFrame = addColumnIndex(spark, unionFrame)

    // store data frame to HBase
    unionFrame.write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    // Show info of raws which their source or dest is "Los Angeles"
    unionFrame.filter(unionFrame("routeName").contains("Los Angeles"))
      .select("driverName", "eventType", "routeName")
      .show()

    // Update "routeName" of the forth raw
    unionFrame.filter(unionFrame("id") === 4)
      .withColumn("routeName", when(col("routeName") === "Santa Clara to San Diego", "Los Angeles to Santa Clara"))
      .toDF()
      .write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def addColumnIndex(spark: SparkSession, df: DataFrame): DataFrame = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index + 1)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("id", LongType, nullable = false)))
  }
}
