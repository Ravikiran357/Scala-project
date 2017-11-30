package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object HotcellAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  // Looking up the number of points that are similar in the spatial cube
  def getValue(similarPointsMap: scala.collection.mutable.Map[String, String],
               map : Map[(String),Long], xyzMinKey:String) : Long = {
    var count = 0l
    if (similarPointsMap.contains(xyzMinKey)) {
      var cell = similarPointsMap(xyzMinKey)
      count = map(cell)
    }
    count
  }

  // Given a tuple(x,y,z), finding it's spatial distance to nearest neighbors and the number of neighboring points
  def getW(similarPointsMap:scala.collection.mutable.Map[String,String], map : Map[String,Long],
           x: Int, y: Int, z: Int) : (Long , Int) = {
    var sum = 0.toLong
    var cellNeighbourCount = 0
    for (i <- -1 to 1; j <- -1 to 1; k <- -1 to 1) {
      var xyzkey = "%d%d%d".format(x + k, y + j, z + i)
      sum += getValue(similarPointsMap, map, xyzkey)
      cellNeighbourCount += 1
    }
    (sum, cellNeighbourCount)
  }

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))

    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5)," +
      " CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
    val givenRectangle = "%d,%d,%d,%d".format(minX.toInt,minY.toInt,maxX.toInt,maxY.toInt)

    pickupInfo.createOrReplaceTempView("pickupInfo")
    // using the max,min of x,y to create a rectangular boundary to eliminate outliers
    spark.udf.register("ST_Contains",(x:Int, y:Int, minX: Int, minY: Int, maxX: Int, maxY: Int)=>
      HotcellUtils.ST_Contains(x, y, minX, minY, maxX, maxY))
    pickupInfo = spark.sql("select x,y,z from pickupInfo where ST_Contains(x,y," +
      givenRectangle + ")")
    pickupInfo.createOrReplaceTempView("pickupInfo")
    pickupInfo = spark.sql("select x,y,z,count(*) as count from pickupInfo GROUP BY x,y,z")

    // Using this list to populate the required mappings
    val list = pickupInfo.collect()
    // This map holds the distinct tuples (x,y,z) and the count of their neighboring points in the cell
    var map:Map[String,Long] = Map()
    // This map holds the normalized points (x,y,z) and connects to the actual, respective data tuple
    var pointsOnSimilarXYZ:scala.collection.mutable.Map[String, String] =
      scala.collection.mutable.Map()
    // Using set to remove redundant looping
    var listX = scala.collection.mutable.Set[Int]()
    var listY = scala.collection.mutable.Set[Int]()
    var listZ = scala.collection.mutable.Set[Int]()
    var countSquared = 0l
    var countsInCell = 0l
    // Populating the data structures
    for (row <- list) {
      var x = row.getInt(0)
      var y = row.getInt(1)
      var z = row.getInt(2)
      var cellCount = row.getLong(3)
      var dataKey = "%d%d%d".format(x,y,z)
      map += (dataKey -> cellCount)
      countSquared += cellCount * cellCount
      countsInCell += cellCount
      var minKey = "%d%d%d".format(x-minX.toInt, y-minY.toInt, z-minZ)
      pointsOnSimilarXYZ.update(minKey, dataKey)
      listX += x
      listY += y
      listZ += z
    }

    val x_bar = countsInCell.toDouble/numCells.toDouble
    val val_S = Math.sqrt(countSquared.toDouble/numCells.toDouble - x_bar*x_bar)

    var minxi = minX.toInt
    var minyi = minY.toInt
    var minzi = minZ.toInt
    var finalMap:Map[(Int,Int,Int),Double] = Map()
    // Finding the Getis-ord statistic using all the
    for (i <- listX; j <- listY; k <- listZ) {
      val (wijxj, wij) = getW(pointsOnSimilarXYZ, map,i-minxi,j-minyi,k-minzi)
      val numerator = wijxj - (wij * x_bar)
      val denominator = val_S * math.sqrt((numCells * wij - (wij * wij)) / (numCells - 1))
      finalMap += (i,j,k) -> numerator/denominator
    }
    val abc = finalMap.toSeq.sortBy(- _._2)
    val finalResult = abc.take(50)
    val r1 = finalResult.map{ case (k,v) => k}
    import spark.implicits._
    val df = r1.toDF()

    return df
  }
}
