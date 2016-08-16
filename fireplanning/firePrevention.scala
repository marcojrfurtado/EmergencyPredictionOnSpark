
/**
 * NOTE: SparkContext is available as sc.
**/
import _root_.hex.deeplearning.DeepLearningModel


import org.apache.spark.SparkFiles
import org.apache.spark.h2o._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import water.fvec.H2OFrame
import water.support.{H2OFrameSupport, SparkContextSupport, ModelMetricsSupport}
import datautils.RefineDateColumn


// DATA Location
val dataFolder = "hdfs://10.0.45.22:9000/user/opuser/seattle-data/"
val emergencyFileName = "real_time_911_calls_SHORT.csv"
val cityFeatureMapFileName = "my_neighborhood_map.csv"


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

// LOad fire station information
val neighbourhoodMapTable = asDataFrame(createTable(dataFolder+cityFeatureMapFileName))(sqlContext)
val fireStationTable = neighbourhoodMapTable.filter(col("City_Feature").like("Fire Stations"))

// Load 911 call data
val emergencyTable = asDataFrame(create911Table(dataFolder+emergencyFileName,"MM/dd/yyyy hh:mm:ss a Z", "Etc/UTC"))(sqlContext)

// Merge both datasets
import datautils.MergeEmergencyCalls._
val emergencyMatchingCalls = emergency_match(emergencyTable,fireStationTable)
emergencyMatchingCalls.registerTempTable("emergencyMatchingTable")
// Group data before classification and generate negative examples
val allTypes = emergencyMatchingCalls.select("Type").dropDuplicates() // select unqiue types for left joint
val allStations = emergencyMatchingCalls.select("Common_Name").dropDuplicates()
val allMonths = sc.parallelize(Range(1,13)).toDF("Month")

allTypes.registerTempTable("allTypes")
allStations.registerTempTable("allStations")
allMonths.registerTempTable("allMonths")

val emergencyMatchingCallsGrouped = sqlContext.sql(
 """SELECT COUNT(e.IncidentNumber) as Ocurrences, t.Type, s.Common_Name, m.Month
    |FROM allTypes t, allStations s, allMonths m
    |LEFT JOIN emergencyMatchingTable e
    |   ON e.Type = t.Type 
    |   AND e.Common_Name = s.Common_Name
    |   AND e.Month = m.Month
    |GROUP BY t.Type, s.Common_Name, m.Month
 """.stripMargin)




// Publish as H2O Frame
emergencyMatchingCallsGrouped.printSchema()
val emergencyMatchGroupDF:H2OFrame = emergencyMatchingCallsGrouped
// Transform all string columns into categorical
H2OFrameSupport.allStringVecToCategorical(emergencyMatchGroupDF)



// Split final data table
val keys = Array[String]("train.hex", "test.hex")
val ratios = Array[Double](0.8, 0.2)
val frs = H2OFrameSupport.splitFrame(emergencyMatchGroupDF, keys, ratios)
val (train, test) = (frs(0), frs(1))


// Define learning model
def DLModel(train: H2OFrame, test: H2OFrame, response: String)
	   (implicit h2oContext: H2OContext) : DeepLearningModel = {
  import _root_.hex.deeplearning.DeepLearning
  import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters

  val dlParams = new DeepLearningParameters()
  dlParams._train = train
  dlParams._valid = test
  dlParams._response_column = response
  dlParams._variable_importances = true
  dlParams._epochs = 100000
  // Create a job
  val dl = new DeepLearning(dlParams)
  val model = dl.trainModel.get
  model
}


//
// Build Deep Learning model
//
val dlModel = DLModel(train, test, 'Ocurrences) 
