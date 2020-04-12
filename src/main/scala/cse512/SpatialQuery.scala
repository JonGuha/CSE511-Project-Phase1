package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
        var points: Array[Double] = pointString.split(",").map(_.toDouble);
        var rectBoundaries: Array[Double] = queryRectangle.split(",").map(_.toDouble);
        checkIfContains(rectBoundaries, points);
      }
      )))

    val resultDf = spark.sql("select * from point where ST_Contains('" + arg2 + "',point._c0)")
    resultDf.show()
    return resultDf.count()
  }

  def checkIfContains(rectBoundaries: Array[Double], points: Array[Double]): Boolean = {
    //bottom co-ordinates  of rectangle
    var left_x = min(rectBoundaries(0), rectBoundaries(2))
    var bottom_y = min(rectBoundaries(1), rectBoundaries(3))

    //top co-ordinates of a rectangle
    var right_x = max(rectBoundaries(0), rectBoundaries(2))
    var top_y = max(rectBoundaries(1), rectBoundaries(3))

    if ( points(0) >= left_x && points(1) >= bottom_y && points(0) <= right_x  && points(1) <= top_y)
    {
      return true;
    }
    else
    {
      return false;
    }
  }


  def calcEuclideanDist(point1: Array[Double], point2: Array[Double]): Double = {
    var sq1: Double = pow(point1(0) - point2(0), 2);
    var sq2: Double = pow(point1(1) - point2(1), 2);
    var ret: Double = sqrt(sq1 + sq2)
    return ret
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => (({
      var rectPoints: Array[Double] = queryRectangle.split(",").map(_.toDouble);
      var points: Array[Double] = pointString.split(",").map(_.toDouble);
      checkIfContains(rectPoints, points);
    })))
    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")

    resultDf.show()
    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => (({
      var point1: Array[Double] = pointString1.split(",").map(_.toDouble);
      var point2: Array[Double] = pointString2.split(",").map(_.toDouble);
      var bool: Boolean = false;
      if (calcEuclideanDist(point1, point2) <= distance) {
        bool = true;
      }
      bool;

    })))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'" + arg2 + "'," + arg3 + ")")
    resultDf.show()
    return resultDf.count()

  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => (({
      var point1: Array[Double] = pointString1.split(",").map(_.toDouble);
      var point2: Array[Double] = pointString2.split(",").map(_.toDouble);
      var bool: Boolean = false;
      if (calcEuclideanDist(point1, point2) <= distance) {
        bool = true;
      }
      bool;
    })))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, " + arg3 + ")")
    resultDf.show()
    return resultDf.count()

  }
}

