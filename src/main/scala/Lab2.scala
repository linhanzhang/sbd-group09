import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._
import collection.JavaConverters._
import org.apache.log4j.{Level, Logger}

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
    val spark = SparkSession
      .builder()
      .appName("Lab 2")
      .config("spark.master", "local")
      .config("spark.sql.broadcastTimeout", "3600") // avoid time-out error
      .config("spark.io.compression.lz4.blockSize", "512kb")
      .config("spark.shuffle.unsafe.file.output.buffer", "1mb")
      .config("spark.shuffle.file.buffer", "1mb")
      .config("spark.executor.memory", "2g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") //stop DEBUG and INFO messages
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    //  change the log levels
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val height = typecheck.matchFunc(args(0))
    if (height == 0 || height == -1) {
      println("******************************************************")
      println("Invalid input, program terminated")
      spark.stop
    } else {
      println("******************************************************")
      println("        The sea level rised " + height + " m          ")
      val (placeDF, harbourDF) = readOpenStreetMap(
        spark.read.format("orc").load("netherlands-latest.osm.orc")
      ); //complete osm dataset
      val elevationDF = readALOS(spark.read.load("parquet/*")); //complete alos dataset

      // ************** combine two datasets with H3 ************************
      val (floodDF, safeDF) = combineDF(
        placeDF,
        elevationDF.select(col("H3"), col("elevation")),
        height
      )
      // *************** find the closest destination *************
      findClosestDest(floodDF, safeDF, harbourDF)
      // Stop the underlying SparkContext0
      spark.stop
    }
  }

  def readOpenStreetMap(df: DataFrame): (DataFrame, DataFrame) = {

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

    // ********** make the keys to be column names *************
    val groupdf = splitTagsDF
      .groupBy("id", "lat", "lon")
      .pivot("key", Seq("name", "place", "population", "harbour"))
      .agg(first("value"))

    /*
	  root
	  |-- id: long (nullable = true)
	  |-- type: string (nullable = true)
	  |-- lat: decimal(9,7) (nullable = true)
	  |-- lon: decimal(10,7) (nullable = true)
	  |-- name: string (nullable = true)
	  |-- place: string (nullable = true)
	  |-- population: string (nullable = true)
	  |-- harbour: string (nullable = true)
+-------+--------+----+----+--------------+-----+----------+-------+
|id     |type    |lat |lon |name          |place|population|harbour|
+-------+--------+----+----+--------------+-----+----------+-------+
|144640 |relation|null|null|Hooglanderveen|null |null      |null   |
|333291 |relation|null|null|Bus 73: Maarss|null |null      |null   |
|358048 |relation|null|null|Bus 102: Utrec|null |null      |null   |
     */

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
      .cache() // this is for dividing the places into groups, and the calculation of distances will be done within each groups
    //h3mapdf.show(50,false)

    //***********separate the harbours and other places *******************
    val harbourDF = h3mapdf
      .filter(col("harbour") === "yes")
      .withColumn("H3RoughArray", neighbourUDF(col("lat"), col("lon"), lit(0)))
      .select(col("H3").as("harbourH3"), col("H3RoughArray"))
    //.cache()

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
    //.cache()
    //   println("placeDF")
    //  placeDF.show(50,false)
    println("******************************************************")
    println("* Finished building up DAG for reading OpenStreetMap *")

    return (placeDF, harbourDF)
    /*
+----------+----+----------+---------+--------+-------+-----+---------------+---------------+
|id        |type|lat       |lon      |name    |place  |popu |H3             |H3Rough        |
+----------+----+----------+---------+--------+-------+-----+---------------+---------------+
|44843991  |node|52.0102642|5.4332757|Leersum |village|7511 |8a1969053247fff|85196907fffffff|
|44710922  |node|51.9810496|5.1220284|Hageste |village|1455 |8a196972e56ffff|85196973fffffff|
|45568761  |node|52.1746100|5.2909500|Soest   |town   |39395|8a19691890a7fff|8519691bfffffff|
     */
  }
  def readALOS(alosDF: DataFrame): DataFrame = {

    val elevationDF = alosDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
      .select(col("H3"), col("elevation"))
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

    /** ****** Combine osm and alos with h3 value *******
      */
    //combinedDF - name,place,population,H3,H3Rough,min(elevation)
    val combinedDF_pre = placeDF
      .join(elevationDF, Seq("H3"), "inner")
      .cache() // cached for the self-join operation

    val combineMinDF = combinedDF_pre
      .groupBy("name")
      .min("elevation")
      .withColumnRenamed("min(elevation)", "elevation")

    val combinedDF =
      combinedDF_pre // each place with its single elevation reference
        .join(combineMinDF, Seq("name", "elevation"))
        .dropDuplicates("name")
        .cache()
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

    /** ****** find the closest city **********
      */

    //+----------+------------+-----------+----------+---------------+--------+
    //|place     |num_evacuees|destination|dest_place|safe_population|distance| H3Rough floodH3
    //+----------+------------+-----------+----------+---------------+--------+
    //|Bleiswijk |11919       |Delft      |city      |101386         |101     |
    //|Nootdorp  |19160       |Delft      |city      |101386         |35      |
// Instead of directly join the DFs based on H3Rough
// need to check whether the H3Rough of the safe city is included in the flooded H3Rough List
    val floodTocity =
      floodDF //join flood & safe df with H3Rough, calculate the distance between each place and destination
        .join(
          safeDF,
          arrays_overlap(col("floodH3Rough"), col("H3RoughArray")),
          "leftouter"
        )
        // .join(safeDF,Seq("H3"))
        .withColumn("city_distance", distanceUDF(col("floodH3"), col("safeH3")))
        .drop("safeH3", "H3RoughArray")
        .cache()
    // .sort("place")// test result
    // println("floodTocity")
    // floodTocity.show(100, false)

// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |

    println("******************************************************")
    println("*************** find the closest city ****************")
    val minCity = floodTocity
      .groupBy("place")
      .min("city_distance")
      .withColumnRenamed("min(city_distance)", "city_distance")

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

    //  println("closestCity")
    //  closestCity.printSchema()
//    root
//  |-- place: string (nullable = true)
//  |-- city_distance: integer (nullable = true)
//  |-- num_evacuees: integer (nullable = true)
//  |-- destination: string (nullable = true)
//  |-- safe_population: string (nullable = true)

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
        //.drop("H3RoughArray", "floodH3", "harbourH3", "floodH3Rough")
        .drop("harbourH3", "floodH3", "floodH3Rough")
        .cache()

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
        "floodH3Rough"
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
    // floodToCH.printSchema()
    //  .withColumnRenamed("place", "destination")
    // .select(
    //   "num_evacuees",
    //   "harbour_distance",
    //   "destination",
    //   "city_distance",
    //   "safe_population"
    // )
    // println("floodToCH")
    // floodToCH.show(50, false)

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
      .orc("output/data/relocate.orc") // output as .orc file
    println("****************** Finished save *********************")

    /*
     val output_12 = spark.createDataFrame(spark.sparkContent.parallelize(relocate_output),schema) //re-create data with the required schema
     output_12.write.orc("relocate_output_12.orc")

     val testread = spark.read.format("orc").load("output_12.orc")
     */

    // ********* calculate the total number of evacuees to each destination ********
    println("******************************************************")
    println("****** aggregate evacuees by their destination *******")
    val receive_popu = relocate_output
      .groupBy("destination")
      .agg(
        sum("num_evacuees").as("evacuees_received"),
        avg("safe_population").as("old_population")
      );

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
    receive_output.write.mode("overwrite").orc("output/data/receive_output.orc")
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
  // def findNeighbour(h3Index: String, k: Int): List[String] = {
  //   val refPoint = h3.h3ToGeo(h3Index).toString()
  //   var lat = refPoint(0)
  //   var lon = refPoint(1)
  //   var rough_res: Int = 6
  //   val h3Rough = h3.geoToH3Address(lat, lon, rough_res)
  //   return h3.kRing(h3Rough, k).asScala.toList
  // }
  def findNeighbour(lat: Double, lon: Double, k: Int): List[String] = {
    var rough_res: Int = 2
    val h3Rough = h3.geoToH3Address(lat, lon, rough_res)
    return h3.kRing(h3Rough, k).asScala.toList
  }
  def getH3Distance(origin: String, des: String): Int = {
    if (
      des != null
    ) //if no harbour in the hexagon, the distance to harbour will be set to 100000
      //(which is definitely bigger than the distance to any city in that hexagon
      return h3.h3Distance(origin, des)
    else
      return 100000
  }

}

