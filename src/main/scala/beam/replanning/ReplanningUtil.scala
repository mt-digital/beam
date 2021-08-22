package beam.replanning

import beam.router.model.EmbodiedBeamTrip
import beam.utils.DebugLib
import com.typesafe.scalalogging.LazyLogging
import org.matsim.api.core.v01.population._
import org.matsim.core.config.groups.PlanCalcScoreConfigGroup
import org.matsim.core.population.PopulationUtils
import org.matsim.core.replanning.selectors.RandomPlanSelector

import scala.collection.JavaConverters._

object ReplanningUtil extends LazyLogging {

  def makeExperiencedMobSimCompatible[T <: Plan, I](person: HasPlansAndId[T, I]): Unit = {
    val experiencedPlan = person.getSelectedPlan.getCustomAttributes
      .get(PlanCalcScoreConfigGroup.EXPERIENCED_PLAN_KEY)
      .asInstanceOf[Plan]

    if (experiencedPlan != null && experiencedPlan.getPlanElements.size() > 0) {
      // keep track of the vehicles that been used during previous simulation
      for (i <- 0 until (experiencedPlan.getPlanElements.size() - 1)) {
        experiencedPlan.getPlanElements.get(i) match {
          case leg: Leg =>
            try {
              val planElements = person.getSelectedPlan.getPlanElements
              val selectedPlanElement = planElements.get(i)
              val vehiclesOfSelectedPlanElement = selectedPlanElement.getAttributes.getAttribute("vehicles")
              // Make sure it is not `null`
              Option(x = vehiclesOfSelectedPlanElement).foreach { attibValue =>
                leg.getAttributes.putAttribute("vehicles", attibValue)
              }
            } catch {
              case ex: Throwable =>
                val text1 = s"Selected size: ${person.getSelectedPlan.getPlanElements.size()}"
                val text2 = s"Experienced size: ${experiencedPlan.getPlanElements.size()}"
                logger.error(s"Length of plans is not the same. $text1. $text2.")
                logger.error(s"The person: ${person.toString}")
                logger.error(s"Selected plans:")
                person.getSelectedPlan.getPlanElements.forEach(pElem => logger.error(pElem.getAttributes.toString))
                logger.error(s"Experienced plans:")
                experiencedPlan.getPlanElements.forEach(pElem => logger.error(pElem.getAttributes.toString))
                logger.error(s"The exception: ${ex.toString}")
                throw ex
            }
          case _ =>
        }
      }
      // BeamMobsim needs activities with coords
      val plannedActivities =
        person.getSelectedPlan.getPlanElements.asScala.filter(e => e.isInstanceOf[Activity])
      val experiencedActivities =
        experiencedPlan.getPlanElements.asScala.filter(e => e.isInstanceOf[Activity])
      plannedActivities.zip(experiencedActivities).foreach {
        case (plannedActivity: Activity, experiencedActivity: Activity) =>
          experiencedActivity.setCoord(plannedActivity.getCoord)
          experiencedActivity.setEndTime(plannedActivity.getEndTime)
        case (_, _) =>
      }
      val attributes = experiencedPlan.getAttributes
      val modalityStyle = if (person.getSelectedPlan.getAttributes.getAttribute("modality-style") == null) { "" }
      else {
        person.getSelectedPlan.getAttributes.getAttribute("modality-style")
      }
      val scores = if (person.getSelectedPlan.getAttributes.getAttribute("scores") == null) { "" }
      else {
        person.getSelectedPlan.getAttributes.getAttribute("scores")
      }
      attributes.putAttribute("modality-style", modalityStyle)
      attributes.putAttribute("scores", scores)
      assert(experiencedPlan.getPlanElements.get(0).asInstanceOf[Activity].getCoord != null)

      copyRemainingPlanElementsIfExperiencedPlanIncomplete(person.getSelectedPlan, experiencedPlan)

      person.asInstanceOf[Person].addPlan(experiencedPlan)
      person.removePlan(person.getSelectedPlan)
      person.asInstanceOf[Person].setSelectedPlan(experiencedPlan)
    }
  }

  def copyRemainingPlanElementsIfExperiencedPlanIncomplete(originalPlan: Plan, experiencedPlan: Plan): Unit = {

    if (originalPlan.getPlanElements.size() > experiencedPlan.getPlanElements.size()) {
      DebugLib.emptyFunctionForSettingBreakPoint()
      for (i <- experiencedPlan.getPlanElements.size() until originalPlan.getPlanElements.size()) {
        originalPlan.getPlanElements.get(i) match {
          case activity: Activity =>
            experiencedPlan.addActivity(
              PopulationUtils.createActivity(activity)
            )
          case _ =>
            val newLeg = PopulationUtils.createLeg(originalPlan.getPlanElements.get(i).asInstanceOf[Leg])
            Option(originalPlan.getPlanElements.get(i).getAttributes.getAttribute("vehicles")).foreach { attribValue =>
              newLeg.getAttributes.putAttribute("vehicles", attribValue)
            }
            experiencedPlan.addLeg(newLeg)
        }
      }
      DebugLib.emptyFunctionForSettingBreakPoint()
    }

  }

  def copyRandomPlanAndSelectForMutation(person: Person): Unit = {
    person.setSelectedPlan(new RandomPlanSelector().selectPlan(person))
    val newPlan = PopulationUtils.createPlan(person.getSelectedPlan.getPerson)
    PopulationUtils.copyFromTo(person.getSelectedPlan, newPlan)
    person.addPlan(newPlan)
    person.setSelectedPlan(newPlan)
  }

  def addBeamTripsToPlanWithOnlyActivities(originalPlan: Plan, trips: Vector[EmbodiedBeamTrip]): Plan = {
    val newPlan = PopulationUtils.createPlan(originalPlan.getPerson)
    val tripsLength = trips.length
    for (i <- 0 until originalPlan.getPlanElements.size() - 1) {
      newPlan.getPlanElements.add(originalPlan.getPlanElements.get(i))
      if (tripsLength > i) {
        val newLeg = PopulationUtils.createLeg(trips(i).tripClassifier.value)
        newPlan.getPlanElements.add(newLeg)
      }
    }
    newPlan.getPlanElements.add(originalPlan.getPlanElements.get(originalPlan.getPlanElements.size() - 1))
    newPlan
  }

  def addNoModeBeamTripsToPlanWithOnlyActivities(originalPlan: Plan): Plan = {
    val newPlan = PopulationUtils.createPlan(originalPlan.getPerson)
    for (i <- 0 until originalPlan.getPlanElements.size() - 1) {
      newPlan.getPlanElements.add(originalPlan.getPlanElements.get(i))
      val newLeg = PopulationUtils.createLeg("")
      newPlan.getPlanElements.add(newLeg)
    }
    newPlan.getPlanElements.add(originalPlan.getPlanElements.get(originalPlan.getPlanElements.size() - 1))
    newPlan
  }
}
