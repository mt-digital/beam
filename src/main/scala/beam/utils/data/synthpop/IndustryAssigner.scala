package beam.utils.data.synthpop

import beam.sim.common.GeoUtils
import beam.utils.ProfilingUtils
import beam.utils.csv.{CsvWriter, GenericCsvReader}
import beam.utils.data.ctpp.models.ResidenceToWorkplaceFlowGeography
import beam.utils.data.ctpp.readers.BaseTableReader.{CTPPDatabaseInfo, PathToData}
import beam.utils.data.ctpp.readers.flow.IndustryTableReader
import beam.utils.scenario.generic.readers.CsvPlanElementReader
import org.matsim.api.core.v01.Coord

class IndustryAssigner {}

object IndustryAssigner {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Expected two args: 1) path to CTPP 2) Path to plans")
    val pathToCTPP: String = args(0) // "d:/Work/beam/CTPP/"
    val pathToPlans: String = args(1) // "D:/Work/beam/NewYork/results_07-10-2020_22-13-14/plans.csv.gz"
    val databaseInfo = CTPPDatabaseInfo(PathToData(pathToCTPP), Set("36", "34"))

    val odList = new IndustryTableReader(databaseInfo, ResidenceToWorkplaceFlowGeography.`TAZ To TAZ`).read()
    println(s"Read ${odList.size} OD pairs from industry table")


    val odToIndustrySeq = odList
      .groupBy { od =>
        (od.source, od.destination)
      }
      .toSeq
      .map { case (key, xs) => key -> xs.toArray }
      .sortBy { case (key, xs) => -xs.length }
    println(s"odToIndustrySeq: ${odToIndustrySeq.size}")

    val odToIndustryMap = odToIndustrySeq.toMap

    val homeGeoIdToWorkGeoIdWithCounts: Seq[((String, String), Int)] =
      if (true) readFromPlans(pathToPlans) else readFromCsv("homeGeoIdToWorkGeoIdWithCounts.csv")
    println(s"homeGeoIdToWorkGeoIdWithCounts ${homeGeoIdToWorkGeoIdWithCounts.size}")

    writeToCsv(homeGeoIdToWorkGeoIdWithCounts)

    val nKeyIsNotFound = homeGeoIdToWorkGeoIdWithCounts.count { case (key, _) => !odToIndustryMap.contains(key) }
    println(s"nKeyIsNotFound: ${nKeyIsNotFound}")

    homeGeoIdToWorkGeoIdWithCounts.foreach { case (key, totalNumberOfPeople) =>
      odToIndustryMap.get(key) match {
        case None =>
        case Some(industries) =>
          val total = industries.map(_.value).sum
          println(s"totalNumberOfPeople: $totalNumberOfPeople, total of industries: $total")
          println(s"Industries: ${industries.mkString(" ")}")

      }

    }

  }

  private def readFromPlans(pathToPlans: String) = {
    val homeWorkActivities = ProfilingUtils.timed("Read plans", println) {
      CsvPlanElementReader
        .read(pathToPlans)
        .filter { plan =>
          plan.planElementType.equalsIgnoreCase("activity") && plan.activityType.exists(
            act => act.equalsIgnoreCase("home") || act.equalsIgnoreCase("Work")
          )
        }
    }
    println(s"Read ${homeWorkActivities.length} home-work activities")

    val homeGeoIdToWorkGeoId = homeWorkActivities
      .groupBy(plan => plan.personId.id)
      .filter { case (_, xs) => xs.length >= 2 }
      .toSeq
      .map {
        case (_, xs) =>
          // First activity is home, so we can get its geoid
          val homeGeoId = xs(0).geoId.get.replace("-", "")
          // The second activity is work
          val workGeoId = xs(1).geoId.get.replace("-", "")
          ((homeGeoId, workGeoId), 1)
      }

    val homeGeoIdToWorkGeoIdWithCounts = homeGeoIdToWorkGeoId
      .groupBy { case ((o, d), _) => (o, d) }
      .toSeq
      .map {
        case ((o, d), xs) =>
          ((o, d), xs.map(_._2).sum)
      }
      .sortBy(x => -x._2)
    homeGeoIdToWorkGeoIdWithCounts
  }

  private def readFromCsv(path: String): Seq[((String, String), Int)] = {
    def mapper(rec: java.util.Map[String, String]): ((String, String), Int) = {
      val origin = rec.get("origin_geoid")
      val destination = rec.get("destination_geoid")
      val count = rec.get("count").toInt
      ((origin, destination), count)
    }
    val (it, toClose) = GenericCsvReader.readAs[((String, String), Int)](path, mapper, _ => true)
    try {
      it.toVector
    } finally {
      toClose.close()
    }
  }

  private def writeToCsv(homeGeoIdToWorkGeoIdWithCounts: Seq[((String, String), Int)]): Unit = {
    val csvWriter =
      new CsvWriter("homeGeoIdToWorkGeoIdWithCounts.csv", Array("origin_geoid", "destination_geoid", "count"))
    homeGeoIdToWorkGeoIdWithCounts.foreach {
      case ((origin, dest), count) =>
        csvWriter.write(origin, dest, count)
    }
    csvWriter.close()
  }
}