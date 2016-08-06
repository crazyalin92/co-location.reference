import java.util
import java.util.ArrayList

import com.vividsolutions.jts.geom.{Coordinate, Point}
import com.vividsolutions.jts.util.GeometricShapeFactory
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.spatialOperator.DistanceJoin
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.wololo.jts2geojson.GeoJSONWriter

/**
  * Created by Alina on 14.07.2016.
  */
object reference_feature_main {

  /*Init Spark Context*/
  def initSparkContext(jars_directory:String):Tuple2[SparkContext,SQLContext] =
  {
    /*  Initialize Spark Context*/
    val conf: SparkConf = new SparkConf()
      .setAppName("gdelt")
      .set("spark.executor.memory", "100g")
      .set("spark.driver.memory", "100g")
      .set("spark.driver.maxResultSize","100g")
      .setMaster("spark://10.114.22.10:7077")

    val sc = new SparkContext(conf)

    sc.addJar(jars_directory+"/commons-csv-1.1.jar")
    sc.addJar(jars_directory+"/spark-csv_2.10-1.4.0.jar")
    sc.addJar(jars_directory+"/univocity-parsers-1.5.1.jar")
    sc.addJar(jars_directory+"/geospark-0.2.jar")
    sc.addJar(jars_directory+"/jts-1.13.jar")
    sc.addJar("C:\\Users\\lws4\\Documents\\Scripts\\Master's Project\\reference\\target\\spatial-1.0-SNAPSHOT.jar")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    return (sc,sqlContext)

  }
  def GetNeigborhood (p:Point,list:util.List[Point]):Array[String]=
  {
    val result: ArrayList[Int] = new ArrayList[Int](list.size())
    val items = list.iterator()
    result.add(p.getCoordinate.z.toInt)
    while (items.hasNext) {
      val item = items.next()
      result.add(item.getCoordinate.z.toInt)
    }
    val listStrings=result.toArray().distinct.map(t=>t.toString)

    return listStrings

  }
  def createCircle( x:Double,  y: Double, r: Double):String=
    {
    val shapeFactory = new GeometricShapeFactory();
    val writer = new GeoJSONWriter()
    shapeFactory.setNumPoints(32);
    shapeFactory.setCentre(new Coordinate(x, y));
    shapeFactory.setSize(r * 2);
    return writer.write(shapeFactory.createCircle()).toString+",";
  }

  def toFlatTuple(tuple:Tuple2[Point,util.List[Point]]): TraversableOnce[Tuple2[Int,Int]] =
  {
    var result:Array [Tuple2[Int,Int]]=Array()
    val id=tuple._1.getCoordinate.z.toInt
    val r=tuple._2.listIterator()
    while(r.hasNext)
    {
      val rl=r.next()
      result:+=(id,rl.getCoordinate.z.toInt)
    }

    return result

  }

 /*Save Points with neighbor points*/
  def saveNeighborhoodPoints(neighborhoods:JavaPairRDD[Point,util.List[Point]],output:String)=
  {
    val res=neighborhoods.rdd.map { case (p:Point,list:util.List[Point]) => GetNeigborhood(p,list)}
    res.map(t=>t.mkString(";")).coalesce(1).saveAsTextFile(output)

  }

  /*Join Labeled Points With Origin Dataset*/
  def joinWithGDELT(neighborhoods:JavaPairRDD[Point,util.List[Point]],sqlContext: SQLContext,gdelt_reduced:DataFrame): DataFrame =
  {
    val tuples=neighborhoods.rdd.flatMap(t=>toFlatTuple(t))
    val tuplesRows=tuples.map(t=>Row(t._1,t._2))

    var tuples_scheme = StructType(Array(
      StructField("e_id", IntegerType, false),
      StructField("n_id", IntegerType, false)))

    val neigborsDF=sqlContext.createDataFrame(tuplesRows,tuples_scheme)

    val joinDF=gdelt_reduced.join(neigborsDF,gdelt_reduced.col("GLOBALEVENTID")===neigborsDF.col("n_id"))
      .sort("e_id")
      .select("e_id","EventCode")

    return joinDF

  }
  /*Save Transactions*/
  def saveTransactions(DF:DataFrame,eventCode:String,output:String)=
  {
    DF
      .rdd.map(t=>(t(0),t(1).toString))
      .groupByKey()
      .map(t=>(eventCode::t._2.toList).distinct)
      .map(t=>t.mkString(" "))
      .coalesce(1)
      .saveAsTextFile(output)

  }

  /*Save GeoJson Circles*/
  def saveGeoJsonCircles(neighborhoods:JavaPairRDD[Point,util.List[Point]],distance:Double,output:String): Unit =
  {
    val writer = new GeoJSONWriter()
    val GeoJson=neighborhoods.rdd.map{
      case (p:Point,list:util.List[Point])=> createCircle(p.getCoordinate.x,p.getCoordinate.y,distance)}
    GeoJson.coalesce(1).saveAsTextFile(output)

  }


  def main(args: Array[String]) {


   /*HDFS directory*/
    val HDFS_directory= "hdfs://10.114.22.10:9000/alina/"

   /*Init Spark Context*/
    val (sc,sqlContext)=initSparkContext(HDFS_directory+"/jars/")

    /*  Extract Data*/

    //val input="hdfs://10.114.22.10:9000/alina/gdelt/2014100[1-4].export.csv";
    //val input="hdfs://10.114.22.10:9000/alina/gdelt/20160525.export.csv";
    // val input=HDFS_directory+"/gdelt/2015[0-9]*.export.csv"
    val input=HDFS_directory+"/gdelt/2015[0-9]*.export.csv"

    val gdelt=new GDELTdata(sqlContext,input)

    /* Extract Specific Country */
    val country="'US'"
    val output=HDFS_directory+"/colocation/reference/gdelt_norm_USA"
    val gdelt_reduced=gdelt.readCountryDataFrame(country,output)

    /*Save as JSON*/
   // gdelt_reduced.coalesce(1).write.json(HDFS_directory+"/colocation/reference/gdelt_reduced_JSON")


    /* Extract Specific Country, Specific Event Codes*/

    val output_ref=HDFS_directory+"/colocation/reference/gdelt_norm_USA_reference"
    val eventCode="014"
    gdelt.readReferenceEventCode(eventCode,country,output_ref).show(10)

    /* Creation of SpatialRDD */
    val inputLocation_1: String = HDFS_directory+"/colocation/reference/gdelt_norm_USA/part-00000"
    val inputLocation_2: String = HDFS_directory+"/colocation/reference/gdelt_norm_USA_reference/part-00000"
    val offset: Int = 2
    val splitter: String = "csv"
    val gridType: String = "X-Y"
    val numPartions_1: Int =350
    val numPartions_2:Int = 35

    var pointRDD_2=new PointRDD(sc,inputLocation_2,offset,splitter,gridType,numPartions_2)
    var pointRDD_1=new PointRDD(sc,inputLocation_1,offset,splitter,gridType,numPartions_1)

    /*Distance Join*/

    val distance=0.5
    val neigborhoods = DistanceJoin.SpatialJoinQueryWithoutIndex(sc,pointRDD_1,pointRDD_2,distance)


    val output_trc=HDFS_directory+"/colocation/reference/neighborEventID"
    val output_points=HDFS_directory+"/colocation/reference/GeoJsonCircles"
    val output_trc_type=HDFS_directory+"/colocation/reference/transactions"

    //saveNeighborhoodPoints(neigborhoods,output_trc)
    //saveGeoJsonCircles(neigborhoods,distance,output_points)

    val joinDF=joinWithGDELT(neigborhoods,sqlContext,gdelt_reduced)
    saveTransactions(joinDF,eventCode,output_trc_type)


  }

}
