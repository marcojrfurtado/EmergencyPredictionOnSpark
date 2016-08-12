package datautils


import datautils.Haversine._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
//import org.apache.spark.sql.SQLContext.implicits._

/**
 * Merges a 911 call database with the set of fire stations in the city.
 */


object MergeEmergencyCalls {

	def emergency_match(emergencyTable:DataFrame,fireStationTable:DataFrame)(implicit sqlContext:SQLContext) = {
		import sqlContext.implicits._		
		
		// Join to get every possible correspondence between fire station and incident 
		fireStationTable.cache()
		val joined = fireStationTable.join(emergencyTable)

		// Compute Distance between stations and incidents (using haversine distance)
		val sqlfunc = udf(haversine _)
		val joined_with_dist = joined.withColumn("Distance", sqlfunc(joined("Station_Latitude"),joined("Station_Longitude"),joined("Incident_Latitude"),joined("Incident_Longitude")))
		joined_with_dist.registerTempTable("emergencyJoined")

		// Compute minimum distance using map reduce
		val min_dist_by_incident = joined_with_dist.map( c => (c.getAs[String]("IncidentNumber"),c.getAs[Double]("Distance")) )
			.reduceByKey(  (a,b) => if (a<b) a else b )
			.toDF("IncidentNumber","MinDist")
		min_dist_by_incident.registerTempTable("minDistByIncident")



		// Only keep stations that have minimum distance from the incident (the output has the same amount of rows as emergencyTable)
		val emergencyMatchingCalls = sqlContext.sql(
			"""SELECT e.Common_Name , e.Type , e.Day, e.Month , e.Year , e.WeekNum, e.WeekDay ,e.Weekend ,e.HourOfDay
				|FROM emergencyJoined as e
				|INNER JOIN minDistByIncident as m
				|ON e.IncidentNumber = m.IncidentNumber AND e.Distance = m.MinDist""".stripMargin)
		
		emergencyMatchingCalls
	}

}
