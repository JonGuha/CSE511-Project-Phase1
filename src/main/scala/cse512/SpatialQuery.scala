package cse512

import org.apache.spark.sql.SparkSession
import scala.math._

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String, pointString:String):Boolean={
    if(Option(queryRectangle).getOrElse("").isEmpty || Option(pointString).getOrElse("").isEmpty)
      return false
    //print(queryRectangle)
    var rectArray = queryRectangle.split(",")
    var rect_x1 = rectArray(0).trim.toDouble
    var rect_y1 = rectArray(1).trim.toDouble
    var rect_x2 = rectArray(2).trim.toDouble
    var rect_y2 = rectArray(3).trim.toDouble
    //print(rect_x1)

    var pointArray = pointString.split(",")
    var point_x = pointArray(0).trim.toDouble
    var point_y = pointArray(1).trim.toDouble

    if (point_x >= rect_x1 && point_x <= rect_x2 && point_y >= rect_y1 && point_y <= rect_y2)
      return true
    else if (point_x <= rect_x1 && point_x >= rect_x2 && point_y <= rect_y1 && point_y >= rect_y2)
      return true
    else
      return false
  }

  def StWithin(pointC: Array[Double], pointP: Array[Double], distanceD:Double):Boolean = {
    var euclideandist: Double = sqrt(pow(pointP(0) - pointC(0), 2) + pow(pointP(1) - pointC(1), 2))
    if (euclideandist <= distanceD)
      return true
    else
      return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
2
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(({
      var pointDf: String = pointString;
      var rectangleDf: String = queryRectangle;
      ST_Contains(pointDf, rectangleDf)
    })))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
      var fixed_point: Array[Double] = pointString2.split(",").map(_.toDouble);
      var dist_point: Array[Double] = pointString1.split(",").map(_.toDouble);
      StWithin(fixed_point,dist_point,distance)
    })))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(({
      var point_P1: Array[Double] = pointString1.split(",").map(_.toDouble);
      var point_P2: Array[Double] = pointString2.split(",").map(_.toDouble);
      StWithin(point_P1,point_P2,distance)
    })))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
