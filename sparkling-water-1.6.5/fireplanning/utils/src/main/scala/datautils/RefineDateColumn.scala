package datautils

import org.apache.spark.h2o.{H2OContext, H2OFrame}
import water.parser.{BufferedString, ParseSetup}
import org.joda.time.{DateTimeZone, MutableDateTime}
import water.MRTask
import org.joda.time.DateTimeConstants._
import org.joda.time.format.DateTimeFormat
import water.fvec.{Chunk, NewChunk, Vec}
import water.parser.{BufferedString, ParseSetup}

/**
 * Adhoc date column refinement.
 *
 * It takes column in the specified format 'MM/dd/yyyy hh:mm:ss a' and refines
 * it into 8 columns: "Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "Season", "HourOfDay"
 */

class RefineDateColumn(val datePattern: String,
                       val dateTimeZone: String) extends MRTask[RefineDateColumn] {
  // Entry point
  def doIt(col: Vec): H2OFrame = {
    val inputCol = if (col.isCategorical) col.toStringVec else col
    val result = new H2OFrame(
      doAll(Array[Byte](Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM), inputCol).outputFrame(
        Array[String]("Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "HourOfDay"),
        Array[Array[String]](null, null, null, null, null, null, null)))
    if (col.isCategorical) inputCol.remove()
    result
  }

  override def map(cs: Array[Chunk], ncs: Array[NewChunk]): Unit = {
    // Initialize DataTime convertor (cannot be done in setupLocal since it is not H2O serializable :-/
    val dtFmt = DateTimeFormat.forPattern(datePattern).withZone(DateTimeZone.forID(dateTimeZone))
    // Get input and output chunks
    val dateChunk = cs(0)
    val (dayNC, monthNC, yearNC, weekNC, weekdayNC, weekendNC, hourNC)
    = (ncs(0), ncs(1), ncs(2), ncs(3), ncs(4), ncs(5), ncs(6))
    val valStr = new BufferedString()
    val mDateTime = new MutableDateTime()
    for(row <- 0 until dateChunk.len()) {
      if (dateChunk.isNA(row)) {
        addNAs(ncs)
      } else {
        // Extract data
        val ds = dateChunk.atStr(valStr, row).toString
        if (dtFmt.parseInto(mDateTime, ds, 0) > 0) {
          val month = mDateTime.getMonthOfYear
          dayNC.addNum(mDateTime.getDayOfMonth, 0)
          monthNC.addNum(month, 0)
          yearNC.addNum(mDateTime.getYear, 0)
          weekNC.addNum(mDateTime.getWeekOfWeekyear)
          val dayOfWeek = mDateTime.getDayOfWeek
          weekdayNC.addNum(dayOfWeek)
          weekendNC.addNum(if (dayOfWeek == SUNDAY || dayOfWeek == SATURDAY) 1 else 0, 0)
          hourNC.addNum(mDateTime.getHourOfDay)
        } else {
          addNAs(ncs)
        }
      }
    }
  }
  private def addNAs(ncs: Array[NewChunk]): Unit = ncs.foreach(nc => nc.addNA())
}
