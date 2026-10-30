package smartagri.ml

import smartagri.config.AppConfig

/**
 * Centralized configuration for the Task 10 crop-stress model -- seed,
 * hyperparameters, and persistence path, none scattered inline
 * (matching this project's established `KalmanConfig`/`IotSimulatorConfig`
 * pattern of one config case class per package).
 */
final case class MlConfig(
    seed: Long,
    numTrees: Int,
    maxDepth: Int,
    probabilityThreshold: Double,
    modelPath: String
)

object MlConfig {

  /**
   * Default configuration.
   *
   *   - `seed = 42`: an arbitrary but FIXED, reported seed (Task 10's
   *     explicit reproducibility requirement) -- not re-rolled per run.
   *   - `numTrees = 100`, `maxDepth = 8`: modest, defensible
   *     RandomForest hyperparameters for a first baseline -- Task 10
   *     explicitly says not to run a large hyperparameter search.
   *   - `probabilityThreshold = 0.5`: Spark ML's own default binary
   *     decision threshold, used as-is per Task 10's explicit
   *     allowance ("given the deadline, default 0.5 is acceptable") --
   *     not tuned on the test set, not tuned at all.
   *   - `modelPath`: derived from `AppConfig.default.hdfs.results` (the
   *     results zone Task 1's `AppConfig` already reserved), not a new
   *     hardcoded machine-specific path.
   */
  def default(config: AppConfig = AppConfig.default): MlConfig = MlConfig(
    seed = 42L,
    numTrees = 100,
    maxDepth = 8,
    probabilityThreshold = 0.5,
    modelPath = s"${config.hdfs.results}/models/crop_stress_rf"
  )
}
