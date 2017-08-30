package beam.agentsim.agents

import akka.actor.Props
import akka.pattern.{ask, pipe}
import beam.agentsim.agents.BeamAgent._
import beam.agentsim.agents.PersonAgent.{Moving, PassengerScheduleEmptyTrigger, Waiting}
import beam.agentsim.agents.RideHailingAgent._
import beam.agentsim.agents.RideHailingManager.{RegisterRideAvailable, RideAvailableAck}
import beam.agentsim.agents.modalBehaviors.DrivesVehicle
import beam.agentsim.agents.modalBehaviors.DrivesVehicle.EndLegTrigger
import beam.agentsim.agents.vehicles.BeamVehicle.{BeamVehicleIdAndRef, BecomeDriver}
import beam.agentsim.agents.vehicles.{PassengerSchedule, VehiclePersonId}
import beam.agentsim.events.SpaceTime
import beam.agentsim.events.resources.vehicle.{ModifyPassengerSchedule, ReservationRequest, ReservationResponse}
import beam.agentsim.scheduler.BeamAgentScheduler.CompletionNotice
import beam.agentsim.scheduler.TriggerWithId
import beam.router.BeamRouter.Location
import beam.router.Modes.BeamMode.CAR
import beam.router.RoutingModel
import beam.router.RoutingModel.{BeamLeg, BeamTrip, EmbodiedBeamLeg, EmbodiedBeamTrip}
import beam.sim.{BeamServices, HasServices}
import org.matsim.api.core.v01.population.Person
import org.matsim.api.core.v01.{Coord, Id}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global

/**
  */
object RideHailingAgent {


  private val logger = LoggerFactory.getLogger(classOf[PersonAgent])

  // syntactic sugar for props creation
  def props(services: BeamServices, rideHailingAgentId: Id[RideHailingAgent], vehicleIdAndRef: BeamVehicleIdAndRef, location: Coord) =
    Props(new RideHailingAgent(rideHailingAgentId, RideHailingAgentData(vehicleIdAndRef, location), services))

  //////////////////////////////
  // RideHailingAgentData Begin... //
  /////////////////////////////
  object RideHailingAgentData {
    //    def apply(): TaxiData = TaxiData()
  }

  case class RideHailingAgentData(vehicleIdAndRef: BeamVehicleIdAndRef, location: Coord) extends BeamAgentData

  case object Idle extends BeamAgentState {
    override def identifier = "Idle"
  }

  case object Traveling extends BeamAgentState {
    override def identifier = "Traveling"
  }

  case class PickupCustomer(confirmation: ReservationResponse, customerId:Id[Person], pickUpLocation: Location, destination: Location, trip2DestPlan: Option[BeamTrip], trip2CustPlan: Option[BeamTrip])

  case class DropOffCustomer(newLocation: SpaceTime)

  case class RegisterRideAvailableWrapper(triggerId: Long)

  def isRideHailingLeg(currentLeg: EmbodiedBeamLeg): Boolean = {
    currentLeg.beamVehicleId.toString.contains("rideHailingVehicle")
  }

  def getRideHailingTrip(chosenTrip: EmbodiedBeamTrip): Vector[RoutingModel.EmbodiedBeamLeg] = {
    chosenTrip.legs.filter(l => isRideHailingLeg(l))
  }

  def isRideHailingTrip(chosenTrip: EmbodiedBeamTrip): Boolean = {
    getRideHailingTrip(chosenTrip).nonEmpty
  }

}

class RideHailingAgent(override val id: Id[RideHailingAgent], override val data: RideHailingAgentData, val beamServices: BeamServices)
  extends BeamAgent[RideHailingAgentData]
    with HasServices
    with DrivesVehicle[RideHailingAgentData] {
  override def logPrefix(): String = s"RideHailingAgent $id: "

  chainedWhen(Uninitialized) {
    case Event(TriggerWithId(InitializeTrigger(tick), triggerId), info: BeamAgentInfo[RideHailingAgentData]) =>
      holdTickAndTriggerId(tick, triggerId)
      val schedule = PassengerSchedule()
      schedule.addLegs(Vector(BeamLeg(tick.toLong, CAR, 0L)))
      data.vehicleIdAndRef.ref ! BecomeDriver(triggerId, id, Some(schedule))
      goto(PersonAgent.Waiting)
  }

  chainedWhen(Waiting) {

    case Event(TriggerWithId(PassengerScheduleEmptyTrigger(tick), triggerId), info) =>
      val rideAvailable = RegisterRideAvailable(self, info.data.vehicleIdAndRef.id, availableSince = SpaceTime(info.data.location, tick.toLong))
      val managerFuture = (beamServices.rideHailingManager ? rideAvailable).mapTo[RideAvailableAck.type].map(result =>
        RegisterRideAvailableWrapper(triggerId)
      )
      managerFuture pipeTo self
      stay()
    case Event(RegisterRideAvailableWrapper(triggerId), info) =>
      beamServices.schedulerRef ! CompletionNotice(triggerId)
      stay()
    case Event(PickupCustomer(confirmation: ReservationResponse, customerId:Id[Person], pickUpLocation: Location, destination: Location, trip2DestPlan: Option[BeamTrip], trip2CustPlan: Option[BeamTrip]), info: BeamAgentInfo[RideHailingAgentData]) =>
//      val req = ReservationRequest(confirmation.requestId, confirmation.response.right.get.departFrom, confirmation.response.right.get.arriveAt, confirmation.response.right.get.reservedVehicle, Id.createPersonId(id))
      val schedule = PassengerSchedule()
      schedule.addLegs(trip2CustPlan.get.legs)  // Adds trip to customer
      schedule.addPassenger(VehiclePersonId(confirmation.response.right.get.passenger,customerId),trip2DestPlan.get.legs)  // Adds trip to destination
//      data.vehicleIdAndRef.ref ! req
      data.vehicleIdAndRef.ref ! ModifyPassengerSchedule(schedule)
      stay()

  }


  chainedWhen(Moving) {
    case Event(DropOffCustomer(newLocation), info: BeamAgentInfo[RideHailingAgentData]) =>
      beamServices.rideHailingManager ? RegisterRideAvailable(self, info.data.vehicleIdAndRef.id, availableSince = newLocation)
      goto(Idle) using BeamAgentInfo(id, info.data.copy(location = newLocation.loc))
  }


  //// BOILERPLATE /////

  when(Waiting) {
    case ev@Event(_, _) =>
      handleEvent(stateName, ev)
    case msg@_ =>
      logError(s"Unrecognized message $msg")
      goto(Error)
  }

  when(Moving) {
    case ev@Event(_, _) =>
      handleEvent(stateName, ev)
    case msg@_ =>
      logError(s"Unrecognized message $msg")
      goto(Error)
  }

  when(AnyState) {
    case ev@Event(_, _) =>
      handleEvent(stateName, ev)
    case msg@_ =>
      logError(s"Unrecognized message $msg")
      goto(Error)
  }


  //// END BOILERPLATE ////
  /*
   * Helper methods
  def logInfo(msg: String): Unit = {
    //    log.info(s"PersonAgent $id: $msg")
  }

  def logWarn(msg: String): Unit = {
    log.warning(s"PersonAgent $id: $msg")
  }

  def logError(msg: String): Unit = {
    log.error(s"PersonAgent $id: $msg")
  }

  private def publishPathTraversal(event: PathTraversalEvent): Unit = {
    if(beamConfig.beam.events.pathTraversalEvents contains event.mode){
      agentSimEventsBus.publish(MatsimEvent(event))

    }
  }
   */

}


