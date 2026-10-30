package smartagri.decision

/**
 * Minimum viable decision-support layer (Task 12): a deterministic rule
 * mapping a predicted stress probability and its conformal
 * prediction-set ambiguity to one of three agricultural actions.
 *
 * This is a DECISION-SUPPORT DEMONSTRATION, not an autonomous farming
 * prescription and not a validated agronomic recommendation system --
 * the thresholds below are illustrative, documented choices, not
 * calibrated against any real agronomic outcome data.
 */
object DecisionEngine {

  sealed trait Action
  case object LowRisk extends Action
  case object Monitor extends Action
  case object InvestigatePriority extends Action

  val MonitorProbabilityThreshold: Double = 0.4
  val InvestigateProbabilityThreshold: Double = 0.6

  final case class DecisionInput(
      sampleId: String,
      predictedProbability: Double,
      conformalSetSize: Int,
      kalmanReliability: Option[Double] = None
  )

  final case class Decision(sampleId: String, action: Action, reason: String)

  /**
   * Deterministic decision rule, in a fixed, documented order:
   *
   *   1. A conformal set of size 2 (genuinely ambiguous, per Task 11's
   *      calibrated uncertainty) always escalates AT LEAST to MONITOR,
   *      regardless of the point probability estimate.
   *   2. Otherwise (a confident, size-1 conformal set), the decision
   *      follows the predicted probability thresholds directly.
   *   3. High probability AND ambiguity together escalate to
   *      INVESTIGATE/PRIORITY ACTION.
   *
   * `kalmanReliability`, if supplied, is NOT currently used to change
   * the decision -- Task 9 established that Kalman's soil-moisture state
   * operates on IoT streaming data keyed by sensor/event-time, while this
   * decision operates on historical Sentinel-date ML samples; the two
   * aren't naturally joined on a common identity in this project as it
   * stands. The parameter is accepted and documented so a future task
   * that establishes that join can extend this rule without changing its
   * signature, but no unjustified fusion is invented now.
   */
  def decide(input: DecisionInput): Decision = {
    val ambiguous = input.conformalSetSize >= 2

    if (input.predictedProbability >= InvestigateProbabilityThreshold && ambiguous) {
      Decision(input.sampleId, InvestigatePriority,
        s"high predicted probability (${input.predictedProbability}) AND conformal ambiguity (set size ${input.conformalSetSize})")
    } else if (input.predictedProbability >= InvestigateProbabilityThreshold) {
      Decision(input.sampleId, InvestigatePriority, s"high predicted probability (${input.predictedProbability})")
    } else if (ambiguous) {
      Decision(input.sampleId, Monitor, s"conformal prediction set is ambiguous (size ${input.conformalSetSize})")
    } else if (input.predictedProbability >= MonitorProbabilityThreshold) {
      Decision(input.sampleId, Monitor, s"moderate predicted probability (${input.predictedProbability})")
    } else {
      Decision(input.sampleId, LowRisk, s"low predicted probability (${input.predictedProbability}), confident conformal set")
    }
  }

  def decideAll(inputs: Seq[DecisionInput]): Seq[Decision] = inputs.map(decide)
}
