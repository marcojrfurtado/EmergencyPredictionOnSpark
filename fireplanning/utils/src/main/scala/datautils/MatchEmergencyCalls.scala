package datautils


import datautils.Haversine._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import com.ibm.gpuenabler.CUDAFunction



/**
 * Merges a 911 call database with the set of fire stations in the city.
 */


object MatchEmergencyCalls {

	// Limit number of partitions
	val joinPartitionCap = 70000
	
	val ptxURL = getClass.getResource("/MatchEmergencyCalls.ptx")

	def emergency_match(emergencyTable:DataFrame,fireStationTable:DataFrame,enableGPU:Boolean = true)(implicit sc:SparkContext, sqlContext:SQLContext) = {
		import sqlContext.implicits._
		
	
		// Join to get every possible correspondence between fire station and incident 
		fireStationTable.cache()
		val joined = fireStationTable.join(emergencyTable).coalesce(joinPartitionCap)

		// Compute Distance between stations and incidents (using haversine distance)
		val joined_dist_schema = joined.schema.add("Distance",DoubleType)
		var joined_with_dist = sqlContext.createDataFrame(sc.emptyRDD[Row],joined_dist_schema)
		if ( enableGPU ) { // Distance to be computed using GPU
			import com.ibm.gpuenabler.CUDARDDImplicits._
			val haversineGPU = sc.broadcast(new CUDAFunction("haversine",Array("this"),Array("this"),ptxURL))
			
			val distance_output = joined.map( c => 
				Array(c.getAs[Double]("Station_Latitude"),c.getAs[Double]("Station_Longitude"),c.getAs[Double]("Incident_Latitude"),c.getAs[Double]("Incident_Longitude")) )
				.mapExtFunc((a: Array[Double]) => a(0) * a(1) * a(2) * a(3) ,haversineGPU.value)	
			val joined_dist_rdd = joined.rdd.zip(distance_output).map{ case (rLeft, dRight) => Row.fromSeq(rLeft.toSeq++Seq(dRight))}
			joined_with_dist = sqlContext.createDataFrame(joined_dist_rdd,joined_dist_schema)
		} else { // Distance to be computed using CPU
			val sqlfunc = udf(haversine _)
			joined_with_dist = joined.withColumn("Distance", sqlfunc(joined("Station_Latitude"),joined("Station_Longitude"),joined("Incident_Latitude"),joined("Incident_Longitude")))

		}
		joined_with_dist.registerTempTable("emergencyJoined")
		
		// Compute minimum distance using map reduce
		val min_dist_by_incident = joined_with_dist.map( c => (c.getAs[String]("IncidentNumber"),c.getAs[Double]("Distance")))
			.reduceByKey(  (a,b) => if (a<b) a else b)
			.toDF("IncidentNumber","MinDist")
		min_dist_by_incident.registerTempTable("minDistByIncident")


		// Only keep stations that have minimum distance from the incident (the output has the same amount of rows as emergencyTable)
		// Ignored collumns: e.Day, e.WeekNum, e.WeekDay, e.Weekend, e.HourOfDay
		val emergencyMatchingCalls = sqlContext.sql(
			"""SELECT e.IncidentNumber, e.Common_Name , e.Type , e.Month , e.Year 
				|FROM emergencyJoined as e
				|INNER JOIN minDistByIncident as m
				|ON e.IncidentNumber = m.IncidentNumber AND e.Distance = m.MinDist""".stripMargin)
		
		emergencyMatchingCalls
	}

}
