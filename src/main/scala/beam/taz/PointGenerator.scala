package beam.taz

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.shape.random.RandomPointsInGridBuilder
import org.matsim.api.core.v01.Coord

import scala.annotation.tailrec

trait PointGenerator {
  def generate(geometry: Geometry, nPoints: Int): Seq[Coord]
}

class RandomPointsInGridGenerator(val growthCoeff: Double) extends PointGenerator {
  private val projection: Int = 4326
  private val geometryFactory: GeometryFactory = new GeometryFactory(new PrecisionModel(), projection)

  override def generate(geometry: Geometry, nPoints: Int): Seq[Coord] = {
    val pointsBuilder = new RandomPointsInGridBuilder(geometryFactory)
    pointsBuilder.setExtent(geometry.getEnvelopeInternal)

    @tailrec
    def generate(n: Int): Seq[Coord] = {
      pointsBuilder.setNumPoints(n)
      val multiPoint = pointsBuilder.getGeometry
      val points = (0 until multiPoint.getNumGeometries)
        .map(multiPoint.getGeometryN)
        .filter(geometry.contains)
        .map { x =>
          val coord = x.getCoordinate
          new Coord(coord.getOrdinate(0), coord.getOrdinate(1))
        }

      if (points.size >= nPoints) return points.take(nPoints)

      generate((n * growthCoeff).toInt)
    }

    generate((nPoints * growthCoeff).toInt)
  }
}
