package smartagri.iot

/**
 * Derives a deterministic per-(cell, sensorType) seed from the global
 * simulation seed, so that: (a) the same global seed always produces
 * the same series for the same cell/sensor, (b) a different global seed
 * produces a different series, and (c) different cells/sensor types
 * produce different (not identical) series even under the same global
 * seed (Task 6 requirements: determinism, and "different cells do not
 * always generate identical trajectories").
 *
 * Uses `String.hashCode`, which the Java Language Specification defines
 * by a fixed formula (`s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]`) --
 * this is guaranteed stable across JVM versions and implementations,
 * unlike `Object.hashCode` or `java.util.Objects.hash`, which carry no
 * such guarantee. Relying on it here is therefore safe for
 * reproducibility, and is called out explicitly rather than assumed.
 */
object DeterministicSeed {

  def forCellAndSensor(globalSeed: Long, tile: String, cellRow: Int, cellCol: Int, sensorType: String): Long = {
    var h = globalSeed
    h = h * 31L + tile.hashCode.toLong
    h = h * 31L + cellRow.toLong
    h = h * 31L + cellCol.toLong
    h = h * 31L + sensorType.hashCode.toLong
    h
  }

  /** A second, independently-derived seed for the same (cell, sensorType) -- used to keep event-time
    * jitter generation decoupled from value generation, so consuming random draws for one never
    * shifts the other's sequence (see `EventTimeGenerator`). */
  def forEventTiming(baseSeed: Long): Long = baseSeed * 31L + 0x5DEECE66DL
}

/**
 * Deterministic per-cell "bounded random effect" baseline offset (Task
 * 6's spatial-variability requirement): different cells get different
 * baseline conditions, without copying SoilGrids values (the exact
 * mistake this task is correcting from the legacy `Analytics.scala`).
 *
 * This is intentionally a simple, documented, bounded random offset
 * derived from the cell's own deterministic seed -- NOT a smooth
 * physically-modeled spatial field (e.g. real soil moisture varies
 * smoothly with terrain/soil type in ways this simulator does not
 * attempt to model). The offset is uniform in `[-range, +range]`,
 * deterministic per cell (same seed -> same offset, always), and
 * bounded so it can never push a cell's baseline mean wildly outside
 * the sensor's plausible range on its own.
 */
object SpatialVariation {

  def offsetFor(derivedSeed: Long, range: Double): Double = {
    if (range == 0.0) 0.0
    else {
      val rng = new scala.util.Random(derivedSeed)
      (rng.nextDouble() * 2.0 - 1.0) * range
    }
  }
}
