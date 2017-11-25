package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()


    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5) as x,CalculateY(nyctaxitrips._c5) as y, " +
      "CalculateZ(nyctaxitrips._c1) as z from nyctaxitrips")

    // Define the min and max of x, y, z
    val minY = -74.50/HotcellUtils.coordinateStep
    val maxY = -73.70/HotcellUtils.coordinateStep
    val minX = 40.50/HotcellUtils.coordinateStep
    val maxX = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
    val givenRectangle = printf("%d,%d,%d,%d",minX,minY,maxX,maxY)

    // using the max,min of x,y to create a rectangular boundary to eliminate outliers
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>
      (HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    // joining x,y coordinates
    spark.udf.register("joinCoordinates",(xCoordinate : Int, yCoordinate : Int)=>
      (xCoordinate.toString + "," + yCoordinate.toString))
    println("Before boundary check")
    println(pickupInfo.count())
    pickupInfo = spark.sql("select x,y,z from rectangle,point where ST_Contains(" + givenRectangle
      + ",joinCoordinates(x,y))")

    println("After boundary check")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    println(pickupInfo.count())
    pickupInfo.show()

    // for each row; build another list of lists with each inner list having the values; (x - minX, y - minY, z, later)
    // on this DF , use map, reducebyKey, where key-value in map is ; (x_y_z , 1) and for reduce we get; (x_y_z, count)

    // now create 3d array for the spacetimecube, initialize it using the above DF.

    // then use it to perform rest of calculations




    return pickupInfo // YOU NEED TO CHANGE THIS PART
  }
}
