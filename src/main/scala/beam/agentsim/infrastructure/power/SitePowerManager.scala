package beam.agentsim.infrastructure.power

import beam.agentsim.agents.vehicles.BeamVehicle
import org.matsim.api.core.v01.Id

import scala.collection.concurrent.TrieMap

class SitePowerManager() {

  /**
    * Get required power for electrical vehicles
    *
    * @param vehicles beam vehicles
    * @return power (in joules) over planning horizon
    */
  def getPowerOverPlanningHorizon(vehicles: TrieMap[Id[BeamVehicle], BeamVehicle]): Double =
    vehicles.view
      .filter { case (_, v) => v.beamVehicleType.isEV }
      .map { case (_, v) => v.beamVehicleType.primaryFuelCapacityInJoule - v.primaryFuelLevelInJoules }
      .sum

  /**
    *
    * @param physicalBounds
    * @param vehicles beam vehicles
    * @return map of electrical vehicles withrequired amount of energy in joules
    */
  def replanHorizonAndGetChargingPlanPerVehicle(
    physicalBounds: PhysicalBounds,
    vehicles: TrieMap[Id[BeamVehicle], BeamVehicle]
  ): Map[Id[BeamVehicle], Double] =
    vehicles.view
      .filter { case (_, v) => v.beamVehicleType.isEV }
      // TODO so far it returns the exact required energy
      .map { case (id, v) => (id, v.beamVehicleType.primaryFuelCapacityInJoule - v.primaryFuelLevelInJoules) }
      .toMap
}
