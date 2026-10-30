package smartagri.iot

import java.time.Instant

import scala.util.Random

/**
 * Pure, Spark-independent generation of a deterministic event-time
 * sequence -- no wall-clock time, no unseeded randomness (Task 6:
 * "avoid System.currentTimeMillis()... unseeded global Random").
 */
object EventTimeGenerator {

  /**
   * Generates `count` ISO-8601 UTC instant strings starting at
   * `startEventTime`, `count-1` steps of `intervalSeconds` apart.
   *
   * REGULAR sampling (`jitterSeconds = 0`): every gap is exactly
   * `intervalSeconds`.
   *
   * IRREGULAR sampling (`jitterSeconds > 0`): each gap is
   * `intervalSeconds + jitter`, where `jitter` is drawn uniformly from
   * `[-jitterSeconds, +jitterSeconds]` using the supplied `seed` --
   * `IotSimulatorConfig` already enforces `jitterSeconds < intervalSeconds`,
   * so every gap stays strictly positive and the sequence is always
   * strictly increasing (Task 6: event identity must be handled
   * deterministically -- a non-increasing sequence would risk two
   * readings colliding on the same timestamp).
   */
  def generate(startEventTime: String, intervalSeconds: Long, jitterSeconds: Long, count: Int, seed: Long): Seq[String] = {
    require(count > 0, s"count must be > 0, was $count")
    val start = Instant.parse(startEventTime)
    val rng = new Random(seed)

    val builder = Seq.newBuilder[String]
    var current = start
    var i = 0
    while (i < count) {
      builder += current.toString
      val jitter = if (jitterSeconds > 0) (rng.nextDouble() * 2.0 - 1.0) * jitterSeconds else 0.0
      val step = intervalSeconds + jitter.toLong
      current = current.plusSeconds(step)
      i += 1
    }
    builder.result()
  }
}
