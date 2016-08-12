
/**
 * NOTE: SparkContext is available as sc.
**/

import org.apache.spark.SparkFiles
import org.apache.spark.h2o._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import water.fvec.H2OFrame
import water.support.{H2OFrameSupport, SparkContextSupport, ModelMetricsSupport}
import datautils.RefineDateColumn

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)

// Start H2O services
implicit val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._


//
// H2O Data loader using H2O API
//
def loadData(datafile: String): H2OFrame = new H2OFrame(new java.net.URI(datafile))

//
// Loader for tables
//
def createTable(datafile: String): H2OFrame = {
  val table = loadData(datafile)

  // Rename coordinate columns
  table.rename("Latitude","Station_Latitude")
  table.rename("Longitude","Station_Longitude")
  
// Update names, replace all ' ' by '_'
  val colNames = table.names().map( n => n.trim.replace(' ', '_'))
  table._names = colNames
  
  table.update()
  table
}

def create911Table(datafile: String, datePattern:String, dateTimeZone:String): H2OFrame = {
  val table = loadData(datafile)

  // Drop unecessary columns
  table.remove(Array(0,5))

  // Refine date into multiple columns
  val dateCol = table.vec(1)
  table.add(new RefineDateColumn(datePattern, dateTimeZone).doIt(dateCol))
  // Remove date column
  table.remove(1)
  
  // Rename coordinate columns
  table.rename("Latitude","Incident_Latitude")
  table.rename("Longitude","Incident_Longitude")

  table.update()
  table
}

//
// Load data
//
SparkContextSupport.addFiles(sc,
  "fireplanning/seattle-data/my_neighborhood_map.csv",
  "fireplanning/seattle-data/real_time_911_calls.csv"
)



val neighbourhoodMapTable = asDataFrame(createTable(SparkFiles.get("my_neighborhood_map.csv")))(sqlContext)
val fireStationTable = neighbourhoodMapTable.filter(col("City_Feature").like("Fire Stations"))
//fireStationTable.registerTempTable("fireStations")

val emergencyTable = asDataFrame(create911Table(SparkFiles.get("real_time_911_calls.csv"),"MM/dd/yyyy hh:mm:ss a Z", "Etc/UTC"))(sqlContext)
//emergencyTable.registerTempTable("emergencyTable")

import datautils.MergeEmergencyCalls._
val emergencyMatchingCalls = emergency_match(emergencyTable,fireStationTable)

/*
import datautils.Haversine._
// Now join tables
fireStationTable.cache()
val joined = fireStationTable.join(emergencyTable)

//val distance_coder ( (Double,Double,Double,Double) => Double) : (p1_Lat: Double, p1_Lon: Double, p2_Lat: Double, p2_Lon: Double) {0}
val sqlfunc = udf(haversine _)


val joined_with_dist = joined.withColumn("Distance", sqlfunc(joined("Station_Latitude"),joined("Station_Longitude"),joined("Incident_Latitude"),joined("Incident_Longitude")))
joined_with_dist.registerTempTable("emergencyJoined")

// Compute minimum distance using map reduce
val min_dist_by_incident = joined_with_dist.map( c => (c.getAs[String]("IncidentNumber"),c.getAs[Double]("Distance")) ).reduceByKey(  (a,b) => if (a<b) a else b ).toDF("IncidentNumber","MinDist")
min_dist_by_incident.registerTempTable("minDistByIncident")




val emergencyMatchingCalls = sqlContext.sql(
  """SELECT e.Common_Name , e.Type , e.Day, e.Month , e.Year , e.WeekNum, e.WeekDay ,e.Weekend ,e.HourOfDay
    |FROM emergencyJoined as e
    |INNER JOIN minDistByIncident as m
    |ON e.IncidentNumber = m.IncidentNumber AND e.Distance = m.MinDist""".stripMargin)
*/
//val emRows = emergencyTable.colect()
