package datautils

import water.fvec.H2OFrame
import org.apache.hadoop


/** 
 * Object that contains methods that assist creation of tables
*/


object TableHelpers {
	
	val defaultTZONE = "Etc/UTC"
	val defaultPatternSet : Array[(String,String)] =   Array(("/","MM/dd/yyyy hh:mm:ss a Z"),("-","yyyy-MM-dd'T'HH:mm:ss Z"))
	//
	// H2O Data loader using H2O API
	//
	def loadData(datafile: String): H2OFrame = new H2OFrame(new java.net.URI(datafile))

	//
	// Loader for tables
	//
	def createStationTable(datafile: String): H2OFrame = {
	  val table = loadData(datafile)

	  // Update names, replace all ' ' by '_'
	  val colNames = table.names().map( n => n.trim.replace(' ', '_'))
	  table._names = colNames
	  
	  // Rename coordinate columns
	  table.rename("Latitude","Station_Latitude")
	  table.rename("Longitude","Station_Longitude")
	  table.rename("Common_Name","Station_Name")
	  
	  
	  table.update()
	  table
	}

	def create911Table(datafile: String): H2OFrame = {
	  val table = loadData(datafile)

	  // Drop unecessary columns
	  table.remove(Array(0,5))

	  // Refine date into multiple columns
	  val dateCol = table.vec(1)
	  table.add(new RefineDateColumn(defaultPatternSet, defaultTZONE).doIt(dateCol))
	  // Remove date column
	  table.remove(1)
	  
	  // Rename coordinate columns
	  table.rename("Latitude","Incident_Latitude")
	  table.rename("Longitude","Incident_Longitude")

	  table.update()
	  table
	}

	def persistTable(frame: H2OFrame, path: String)(implicit fs:hadoop.fs.FileSystem) {
	  
	   val out_stream = fs.create(new hadoop.fs.Path(path))
	   val csv_ref_data = frame.toCSV(true,false)
	   Iterator.continually(csv_ref_data.read)
		.takeWhile(-1 !=)
		.foreach(out_stream.write)
	   out_stream.close()
	}
}
