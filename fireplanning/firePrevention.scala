
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

// Execution parameters

// Force all jobs to be exetuted. If false, will try to use cached information
val forceAllJobs = true
// Enables GPU when merging datasets
val enableGPU = false

// Reference Data Location
val dataFolder = "seattle-data/"
val emergencyFileName = "real_time_911_calls_FULL.csv"
val cityFeatureMapFileName = "my_neighborhood_map.csv"


// Cache data location
val matchingCallsTable = "emergencyMatchingTable"
val matchingCallsGroupTable = "emergencyMatchingGroupTable"

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)

// Start H2O services
implicit val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._


// Hadoop configuration
import org.apache.hadoop
val hconf = sc.hadoopConfiguration
val fs = hadoop.fs.FileSystem.get(hconf)


//////////////////////////////////////////////////////////////////////////////////////////
// STEP 1 : Load Seattle Open city data (Emergency call log and fire station information)
//////////////////////////////////////////////////////////////////////////////////////////



import datautils.TableHelpers._

// Load fire station information
val ngMapFilePath = fs.resolvePath(new hadoop.fs.Path(dataFolder+cityFeatureMapFileName))
val neighbourhoodMapTable = asDataFrame(createStationTable(ngMapFilePath.toString()))(sqlContext)
val fireStationTableRaw = neighbourhoodMapTable.filter(col("City_Feature").like("Fire Stations"))
val fireStationTable = fireStationTableRaw.na.drop()

// Load 911 call data
val emFilePath = fs.resolvePath(new hadoop.fs.Path(dataFolder+emergencyFileName))
val emergencyTableRaw = asDataFrame(create911Table(emFilePath.toString()))(sqlContext)
val emergencyTable = emergencyTableRaw.na.drop()

//////////////////////////////////////////////////////////////////////////////////////////
// STEP 2 : Pre-processing: Merge data into a single DataFrame. Needs to compute distance between incidents and stations
//////////////////////////////////////////////////////////////////////////////////////////


// Merge both datasets
import datautils.MatchEmergencyCalls._

// CHeck whether data is cached
var emergencyMatchingCalls : DataFrame = _
val placeHolderDir = fs.resolvePath(new hadoop.fs.Path(dataFolder))
val emMatchingPathStr = placeHolderDir+"/"+matchingCallsTable+".csv"
val matchingCallsFileAvailable = fs.exists(new hadoop.fs.Path(emMatchingPathStr))
if ( !forceAllJobs & matchingCallsFileAvailable ) {
   print("Loading matching between fire stations and incidents...\n")
   emergencyMatchingCalls = asDataFrame(loadData(emMatchingPathStr))
} else {
   print("Generating matching between fire stations and incidents...\n")
   emergencyMatchingCalls = emergency_match(emergencyTable,fireStationTable,enableGPU)(sc,sqlContext)
   
   //Persist DataFrame
   persistTable(asH2OFrame(emergencyMatchingCalls),emMatchingPathStr)(fs)

}
emergencyMatchingCalls.registerTempTable(matchingCallsTable)

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
// STEP 3 : Pre-processing: Group data based on selected features (e.g. Month of incident ). To be used for training purposes
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

var emergencyMatchingCallsGrouped : DataFrame = _
val emMatchingGroupPathStr = placeHolderDir+"/"+matchingCallsGroupTable+".csv"
val matchingGroupFileAvailable = fs.exists(new hadoop.fs.Path(emMatchingGroupPathStr))
if ( !forceAllJobs & matchingGroupFileAvailable ) {
   print("Loading groups of incidents..\n")
   emergencyMatchingCallsGrouped = asDataFrame(loadData(emMatchingGroupPathStr))
} else {
   print("Generating groups of incidents...\n")
   
   emergencyMatchingCallsGrouped = sqlContext.sql(
	 s"""SELECT COUNT(e.IncidentNumber) as Ocurrences, e.Year, e.Month, e.Type, e.Station_Name
	    |FROM $matchingCallsTable e
	    |GROUP BY e.Year, e.Month, e.Type, e.Station_Name
	 """.stripMargin).drop("Year")
   //Persist DataFrame
   persistTable(asH2OFrame(emergencyMatchingCallsGrouped),emMatchingGroupPathStr)(fs)
}

// Publish as H2O Frame
emergencyMatchingCallsGrouped.printSchema()
val emergencyMatchGroupDF:H2OFrame = emergencyMatchingCallsGrouped
// Transform all string columns into categorical
H2OFrameSupport.allStringVecToCategorical(emergencyMatchGroupDF)

//////////////////////////////////////////////////////////////////////////////////////////////////////////////
// STEP 4 : Train our model based on grouped features
//////////////////////////////////////////////////////////////////////////////////////////////////////////////

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


print("Ready to train model...\n")
//
// Build Deep Learning model
//
val dlModel = DLModel(train, test, 'Ocurrences) 
