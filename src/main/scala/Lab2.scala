import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import com.uber.h3core.exceptions.DistanceUndefinedException
import org.apache.spark.sql.SaveMode
import scala.sys.process._
import collection.JavaConverters._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window


object Lab2 {
// converts latitude and longitude data into H3 index
  val geoUDF = udf((lat: Double, lon: Double, res: Int) =>
    h3Helper.toH3func(lat, lon, res)
  )
  val neighbourUDF =
    udf((lat: Double, lon: Double, k: Int) =>
      h3Helper.findNeighbour(lat, lon, k)
    )
// calculates the distance between two places based on h3 toolbox
  val distanceUDF =
    udf((origin: String, des: String) => h3Helper.getH3Distance(origin, des))

  def main(args: Array[String]) {

    // ******** Create a SparkSession  ***************
    // val conf = new SparkConf()
    //   .setAppName("Lab 2")
    //   // .set("spark.kryo.registrationRequired", "true")
    //   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // conf.registerKryoClasses(Array(classOf[Lab2], classOf[h3Helper]))
    // val sparkContext = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Lab 2")
      //.config(conf)
      .getOrCreate()
    // .config("spark.master", "local[*]")
    //.config("spark.sql.broadcastTimeout", "36000") // avoid time-out error
    //.config("spark.io.compression.lz4.blockSize", "512kb")
    //.config("spark.shuffle.unsafe.file.output.buffer", "1mb")
    //.config("spark.shuffle.file.buffer", "1mb")
    //.config("spark.executor.memory", "2g")
    // Assigning executors with a large number of virtual cores leads to
    // a low number of executors and reduced parallelism.
    // Assigning a low number of virtual cores leads to
    // a high number of executors,
    // causing a larger amount of I/O operations.
    //.config("spark.executor.cores", 5)

    spark.sparkContext.setLogLevel("ERROR") //stop DEBUG and INFO messages
    //spark.conf.set("spark.sql.shuffle.partitions", 5)
    //  change the log levels
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    import spark.implicits._

    val height = typecheck.matchFunc(args(0))
    if (height == -1) {
      println("******************************************************")
      println("Invalid input, program terminated")
      spark.stop
    } else {
      println("******************************************************")
      println("        The sea level rised " + height + " m          ")
      val (placeDF, harbourDF) = readOpenStreetMap(
        //spark.read.format("orc").load("s3://abs-tudelft-sbd-2021/france.orc")
        //spark.read.format("orc").load("s3://abs-tudelft-sbd-2021/north-america.orc")
        // spark.read.format("orc").load("s3://abs-tudelft-sbd-2021/europe.orc")
        spark.read.format("orc").load("s3://osm-pds/planet/planet-latest.orc")
      );
      val elevationDF = readALOS(
        spark.read.load("s3://abs-tudelft-sbd-2021/ALPSMLC30.parquet/")
      ); //complete alos dataset

      // ************** combine two datasets with H3 ************************
      val (floodDF, safeDF) = combineDF(
        placeDF,
        elevationDF.select(col("H3"), col("elevation")),
        height
      )
      // // *************** find the closest destination *************
      findClosestDest(floodDF, safeDF, harbourDF)
      // Stop the underlying SparkContext
      spark.stop
    }
  }

  def readOpenStreetMap(df: DataFrame): (DataFrame, DataFrame) = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    // ********* explode and filter the useful tags ************
    val splitTagsDF = df
      .filter(col("type") === "node")
      .select(
        col("id"),
        col("lat"),
        col("lon"),
        explode(col("tags"))
      )
      .filter(
        col("key") === "name" || col("key") === "place" ||
          col("key") === "population" || col("key") === "harbour"
      )
      //.persist(StorageLevels.MEMORY_AND_DISK)

    // ********** make the keys to be column names *************
    val groupdf = splitTagsDF
      .groupBy("id", "lat", "lon")
      .pivot("key", Seq("name", "place", "population", "harbour"))
      .agg(first("value"))

    // ********** remove the rows with imcomplete information *******
    val groupLessDF = groupdf
      .filter(
        (col("place").isNotNull && col("population").isNotNull &&
          (col("place") === "city" || col("place") === "town" || col(
            "place"
          ) === "village" || col("place") === "halmet")) || col(
          "harbour"
        ) === "yes"
      )
    //groupLessDF.show(50,false)

    //********** calculate the coarse/fine-grained H3 value ****************
    val h3mapdf = groupLessDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
    //h3mapdf.show(50,false)

    //***********separate the harbours and other places *******************
    val harbourDF = h3mapdf
      .filter(col("harbour") === "yes")
      .withColumn("H3RoughArray", neighbourUDF(col("lat"), col("lon"), lit(0)))
      .select(col("H3").as("harbourH3"), col("H3RoughArray"))

    //  println("harbourDF")
    //  harbourDF.show(50, false)

    val placeDF = h3mapdf // harbour is filtered out
      .filter(col("harbour").isNull)
      .drop("harbour")
      //.withColumn("H3Rough", geoUDF(col("lat"), col("lon"), lit(5)))
      .select(
        col("name"),
        col("population"),
        col("H3"),
        col("place"),
        col("lat"),
        col("lon")
        //  col("H3Rough")
      )
    // placeDF
    //   .orderBy(asc("H3"))
    //   .write
    //   .format("parquet")
    //   .bucketBy(1188, "H3")
    //   .mode(SaveMode.Overwrite)
    //   .saveAsTable("placeTbl")
    // val placeTblDF =
    // spark.sql("CACHE TABLE placeTbl")
    println("******************************************************")
    
    //   println("placeDF")
    //  placeDF.show(50,false)
    println("******************************************************")
    println("* Finished building up DAG for reading OpenStreetMap *")

    return (placeDF, harbourDF)

  }
  def readALOS(alosDF: DataFrame): DataFrame = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    val elevationH3 = alosDF
      //EU
      // .filter(
      //     col("lat") < 72 && col("lat") > 36 && col("lon") > -9 && col("lon") < 66
      //   )
      // NA
      // .filter(
      //   col("lat") < 81 && col("lat") > 18 && col("lon") > -167 && col("lon") < -20
      // )

      // france
      // .filter(
      //   col("lat") < 52 && col("lat") > 41 && col("lon") > 4 && col("lon") < 9
      // )
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
      .select(col("H3"), col("elevation"))
      
    // val elevationSpecAgg = Window.partitionBy("H3")
    // val elevationSpec = Window.partitionBy("H3").orderBy("H3")
    
    val elevationDF = elevationH3
    // .withColumn("row",row_number.over(elevationSpec))
    // .withColumn("min",min(col("H3")).over(elevationSpecAgg))
    // .where(col("row")===1)
    // .select("H3","elevation")
      .groupBy("H3")
      .min("elevation")
      .withColumnRenamed("min(elevation)", "elevation")

    // elevationDF
    //   .orderBy(asc("H3"))
    //   .write
    //   .format("parquet")
    //   .bucketBy(1188, "H3")
    //   .mode(SaveMode.Overwrite)
    //   .saveAsTable("elevationTbl")
    //val elevationTblDF =
    // spark.sql("CACHE TABLE elevationTbl")

    println("******************************************************")
    println("**** Finished building up DAG for reading ALOSMap ****")
    return elevationDF
  }
  /*combineDF: combine openstreetmap & alos,
           get the relations: name -> lan,lon
           get flooded, safe df
           get the output orc name | evacuees & sum
   */
  def combineDF(
      placeDF: DataFrame,
      elevationDF: DataFrame,
      riseMeter: Int
  ): (DataFrame, DataFrame) = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    /** ****** Combine osm and alos with h3 value *******
      */
    // val placeBucketDF = spark.table("placeTblDF")
    // val elevationBucketDF = spark.table("elevationTblDF")
    //combinedDF - name,place,population,H3,H3Rough,min(elevation)
    // val combinedDF_pre = placeDF
     val combinedDF = placeDF
       .join(elevationDF, Seq("H3"), "inner")
    // val combinedDF = placeBucketDF
    //   .join(elevationBucketDF, Seq("H3"), "inner")
      .persist(
        StorageLevels.MEMORY_AND_DISK
      ) // cached for the the next two operation

    // val combineMinDF = combinedDF_pre
    //   .groupBy("name")
    //   .min("elevation")
    //   .withColumnRenamed("min(elevation)", "elevation")

    // val combinedDF =
    //   combinedDF_pre // each place with its single elevation reference
    //     .join(combineMinDF, Seq("name", "elevation"))
    //     .dropDuplicates("name")
    //     .cache()
    // println("combinedDF")
    // combinedDF.show(50,false)

    /** ********split into flood and safe df **********
      */

    //floodDF: place,num_evacuees, H3, H3Rough
    val floodDF = combinedDF
      .filter(col("elevation") <= riseMeter)
      .drop(
        "elevation",
        "place"
      ) //no need to know the type of flooded place any more
      .withColumn("floodH3Rough", neighbourUDF(col("lat"), col("lon"), lit(1)))
      .drop("lat", "lon")
      .withColumnRenamed("population", "num_evacuees")
      .withColumnRenamed("name", "place")
      .withColumnRenamed("H3", "floodH3")
      .withColumn("num_evacuees", col("num_evacuees").cast("int"))
    //.cache()
    //  println("floodDF")
    //  floodDF.show(50, false)

    val safeDF = combinedDF
      .filter(col("elevation") > riseMeter)
      .drop("elevation")
      .filter(col("place") === "city") //the destination must be a city
      .drop("place")
      // .withColumn("H3Rough", geoUDF(col("lat"), col("lon"), lit(rough_res)))
      .withColumn("H3RoughArray", neighbourUDF(col("lat"), col("lon"), lit(0)))
      .drop("lat", "lon")
      .withColumnRenamed("population", "safe_population")
      .withColumnRenamed("name", "destination")
      .withColumnRenamed("H3", "safeH3")
    //.select(col("destination"),col("safeH3"),col("safe_population"),col("H3Rough"),split(col("H3Rough"), " ").as("H3RoughArray"))
    //.cache()
    // println("safeDF")
    // safeDF.show(50, false)

    return (floodDF, safeDF)

  }

  def findClosestDest(
      floodDF: DataFrame,
      safeDF: DataFrame,
      harbourDF: DataFrame
  ) {

// Instead of directly join the DFs based on H3Rough
// need to check whether the H3Rough of the safe city is included in the flooded H3Rough List
    val k1 = 1
    val floodTocity1 =
      floodDF //join flood & safe df with H3Rough, calculate the distance between each place and destination
        .join(
          safeDF,
          arrays_overlap(col("floodH3Rough"), col("H3RoughArray")),
          "leftouter"
        )
        .cache()
    val matched1 = floodTocity1
      .filter(col("destination").isNotNull)
      .drop("floodH3Rough", "H3RoughArray")
    val unMatched1 = floodTocity1
      .filter("destination is Null")
      .drop(
        "floodH3Rough",
        "H3RoughArray",
        "safeH3",
        "safe_population",
        "destination"
      )
    val floodTocity2 = unMatched1.crossJoin(safeDF).drop("H3RoughArray")
    val floodTocityMatch =
      matched1.union(floodTocity2)
      //.persist(StorageLevels.MEMORY_AND_DISK)

    val floodTocity = floodTocityMatch
      .withColumn("city_distance", distanceUDF(col("floodH3"), col("safeH3")))
      .filter(col("city_distance") >= 0)
      .drop("floodH3Rough", "H3RoughArray", "lat", "lon", "safeH3")
    .persist(StorageLevels.MEMORY_AND_DISK)

    println("******************************************************")
    println("*************** find the closest city ****************")
    val minCity = floodTocity
      .groupBy("place")
      .min("city_distance")
      .withColumnRenamed("min(city_distance)", "city_distance")
      //.persist(StorageLevels.MEMORY_AND_DISK)

    val closestCity = minCity
      .join(
        floodTocity,
        Seq("place", "city_distance")
      ) // join the original dataframe
      .drop("floodH3", "floodH3Rough")
      .dropDuplicates(
        "place",
        "city_distance"
      ) // avoid duplicate due to the same city_distance
    // closestCity.show(50, false)

    /** ***** find the closest harbour ******
      */

    val closestHarbour_pre =
      floodDF //join place,dest with harbour by H3Rough, calculate the distance between each place and harbour
        .join(
          harbourDF,
          arrays_overlap(col("floodH3Rough"), col("H3RoughArray")),
          "leftouter"
        ) //join by H3Rough
        .withColumn(
          "harbour_distance",
          distanceUDF(col("floodH3"), col("harbourH3"))
        )
        .drop("harbourH3", "floodH3", "floodH3Rough")
        .cache()// this DF will be used multiple times in the following process

    val closestHarbour_group = closestHarbour_pre
      .groupBy("place")
      .min("harbour_distance")
      .withColumnRenamed(
        "min(harbour_distance)",
        "harbour_distance"
      ) //place is distinct
    // .cache()

    println("******************************************************")
    println("****** find the distance to the nearest harbour ******")

    val closestHarbour = closestHarbour_pre
      .join(
        closestHarbour_group,
        Seq("harbour_distance", "place")
      )
      .drop(
        "num_evacuees",
        "H3RoughArray",
        "floodH3",
        "floodH3Rough",
        "safeH3"
      ) //for each flooded place, find the distance to the nearest harbour
      .dropDuplicates(
        "place",
        "harbour_distance"
      )
    // println("closestHarbour")
    // closestHarbour.show(50, false)
    //closestHarbour.printSchema()

    val floodToCH = closestCity
      .join(closestHarbour, Seq("place"), "inner")
      .cache()
    /*
      seperate into two dataframes
      |-- near_harbour: places that are closer to a harbour than a safe city
      |-- near_city: places that are closer to a safe city
     */

    //********** divide into 2 DFs ***********
    println("******************************************************")
    println("***** filter out the places closer to a harbour ******")
    val near_harbour = floodToCH
      .filter(col("harbour_distance") <= col("city_distance"))
      .drop("city_distance", "harbour_distance")
      .cache()

    println("******************************************************")
    println("******* filter out the places closer to a city *******")
    val near_city = floodToCH
      .filter(col("harbour_distance") > col("city_distance"))
      .drop("harbour_distance", "city_distance")
    //  near_city.printSchema()
    // ********* operation on <near_harbour> DF **********
    val change_dest =
      near_harbour.withColumn(
        "destination",
        lit("Waterworld")
      ) // change the destination
    val change_popu = change_dest
      .withColumn("num_evacuees", col("num_evacuees") * 0.25)
      . // evacuees to the WaterWorld
      withColumn(
        "safe_population",
        col("safe_population") * 0
      ) // set the population of WaterWorld to 0
    val rest_popu = near_harbour.withColumn(
      "num_evacuees",
      col("num_evacuees") * 0.75
    ) // evacuees to the nearest city
    val near_harbour_new =
      rest_popu.union(change_popu)
    // near_harbour_new.printSchema()
    //.sort("place") // Combined DF
    println("******************************************************")
    println("************ evacuees to harbour and city ************")

    val relocate_output =
      near_harbour_new
        .union(near_city)
        .sort("place") // Combine <near_harbour_new> and <near_city>

    //  relocate_output.printSchema()

    println("******************************************************")
    println("************* output => evacuees by place ************")

    //relocate_output.show(50,false)

    println("******************************************************")
    println("******************* Saving data **********************")
    relocate_output
      .drop("safe_population")
      .write
      .mode("overwrite")
      .orc("s3://group-09/Output/data/relocate") // Cloud output

    // relocate_output
    //   .drop("safe_population")
    //   .write
    //   .mode("overwrite")
    //   .orc("output/data/relocate.orc") // local output
    println("****************** Finished save *********************")

    /*
        val  = spark.read.format("orc").load("output_12.orc")
     */

    // ********* calculate the total number of evacuees to each destination ********
    println("******************************************************")
    println("****** aggregate evacuees by their destination *******")
    val receive_popu = relocate_output
      .groupBy("destination")
      .agg(
        sum("num_evacuees").as("evacuees_received"),
        avg("safe_population").as("old_population")
      )

    /** ******calculate the sum of evacuees*******
      */

    println("******************************************************")
    println("********* calculate total number of evacuees *********")
    val sum_popu = receive_popu
      .groupBy()
      .agg(sum("evacuees_received"))
      .first
      .get(0)

    println("******************************************************")
    println("|        total number of evacuees is " + sum_popu + "      |")
    println("******************************************************")

    // ******* transform the output data into the required format **********
    val receive_output = receive_popu
      .withColumn(
        "new_population",
        col("old_population") + col("evacuees_received")
      )
      .drop("evacuees_received")

    println("******************************************************")
    println("*** output => population change of the destination ***")

    //receive_output.show(50,false)

    println("******************************************************")
    println("******************* Saving data **********************")
    receive_output.write
      .mode("overwrite")
      .orc("s3://group-09/Output/data/receive_output") // Cloud output
    // receive_output.write
    //   .mode("overwrite")
    //   .orc("output/data/receive_output.orc") //Local output
    println("****************** Finished save *********************")

  }
}

object h3Helper {
  val h3 = H3Core.newInstance()

  def toH3func(lat: Double, lon: Double, res: Int): String =
    h3.geoToH3Address(lat, lon, res)
  // Find the closest safe city within the same H3Rough index
  // if not, search the outer circle
  // Until a safe city is found
  // h3Rough is the H3Rough of a flooded place
  // k is initially set to 1, it should be able to increase

  def findNeighbour(lat: Double, lon: Double, k: Int): List[String] = {
    val rough_res: Int = 1
    val h3Rough = h3.geoToH3Address(lat, lon, rough_res)
    return h3.kRing(h3Rough, k).asScala.toList
  }
  def getH3Distance(origin: String, des: String): Int = {
    if (
      des != null
    ) //if no harbour in the hexagon, the distance to harbour will be set to 100000
      //(which is definitely bigger than the distance to any city in that hexagon
      try {
        return h3.h3Distance(origin, des)
      } catch {
        case e: DistanceUndefinedException => -1
      }
    else
      return -1
  }

}
