# Legacy Compatibility Mapping

*Written as part of Phase 3, Task 1 (canonical data model + spatial/temporal foundation). No legacy Python or Scala code was modified or rewritten as part of this task — this document exists to track what will need to change in later migration tasks.*

---

## Spatial identity

**OLD:** `cr` / `cc` (used internally by `Analytics.scala`'s `FI`/`FO` case classes)
**NEW:** `smartagri.spatial.GridCell(tile, cellRow, cellCol)`, always serialized as columns `tile` / `cell_row` / `cell_col`

- This is the specific inconsistency that caused the Stage 4 crash in the audited `Analytics.scala` (`AnalysisException: UNRESOLVED_USING_COLUMN_FOR_JOIN` on `cell_row`): the DataFrame-level join at Stage 1 already correctly uses `cell_row`/`cell_col`, but the internal `FI`/`FO` case classes used at Stage 2 rename them to `cr`/`cc`, so by Stage 4 the `labeled` table only has `cr`/`cc`, not `cell_row`/`cell_col`.
- **Not fixed in this task.** `Analytics.scala` is untouched. This mapping exists so the later fusion-rewrite task (reconciliation blueprint §17 step 4 / §26 step 5) replaces `FI`/`FO`'s `cr`/`cc` fields with `GridCell` (or at minimum renames them to `cellRow`/`cellCol`) instead of reintroducing the same drift.
- `scala-ingestion`'s other modules (`SoilGridsIngestion.scala`, `SentinelIngestion.scala`) already correctly use `cell_row`/`cell_col` at the DataFrame level today — no mapping needed for those.

## Temporal representation

**OLD:** `time_index` (opaque integer, `Era5Ingestion.scala`'s long-format output)
**NEW:** `smartagri.time.CanonicalDate`, serialized as column `date` (ISO-8601 `YYYY-MM-DD`)

- The Python `era5_weather.parquet` already uses a real `date` column and needs no change.
- The Scala `Era5Ingestion.scala` module emits `(variable, time_index, lat, lon, value)` — a long-format table keyed by a positional index into the NetCDF/GRIB time axis, not a real calendar date. **Not fixed in this task.** A later ingestion-migration task needs to decode the actual NetCDF/GRIB time coordinate (calendar + units metadata) into a `CanonicalDate` instead of emitting `time_index`.

## ERA5 spatial key (added — Task 1 correction pass, Correction 2)

**CURRENT:** `smartagri.model.Era5Observation` correctly represents ERA5 on its own native `lat`/`lon` grid — it does NOT carry a `GridCell`, because the existing pipeline does not join ERA5 to the Sentinel-2 grid at ingestion time (see that model's doc comment for the full reasoning).

**REQUIRED FUTURE CONVERSION**, which MUST happen before ERA5 participates in the historical multimodal join:

```
ERA5Observation (native ERA5 lat/lon grid)
        |
        v
spatial sampling / interpolation
        |
        v
canonical Sentinel field grid
        |
        v
GridCell (tile, cell_row, cell_col) + CanonicalDate
```

- **Not implemented in this task.** No sampling/interpolation code exists yet for this step; `kalman_fusion.py` currently does an ad hoc nearest-point lookup at fusion time, which is the closest existing precedent but is not itself the canonical implementation of this step.
- **Hard constraint for whoever implements this:** the sampling step must resolve every ERA5 point against the ONE grid `GridCell`/`SentinelObservation` already define. It must NOT invent a second, ERA5-native row/col grid of its own — that would recreate the exact kind of duplicate-grid problem (see the audit's note on `SentinelIngestion.scala`'s independent pixel-binning grid) that the canonical spatial key exists to prevent.

## IoT definitions

**OLD:** three inconsistent, disconnected "IoT" stand-ins found by the audit:
1. `kalman_fusion.py`'s synthetic ERA5-derived proxy (Python fusion input)
2. `Analytics.scala` Stage 2's use of the static SoilGrids `water_content_fc` as `swvl` (Scala fusion input)
3. `iot_producer.py` → Kafka → `iot_consumer.py`'s actually-simulated stream (built, but never read by either fusion implementation)

**NEW:** `smartagri.model.IotSensorReading` — one schema, to be populated by exactly one future Scala simulator (reconciliation blueprint §5), consumed by exactly one future Spark Structured Streaming job, and used by exactly one future Kalman fusion implementation.

- **Not implemented in this task.** No simulator, no Kafka producer, no Structured Streaming job exists yet — `IotSensorReading` is the agreed-upon target schema so those later tasks build against one already-settled type instead of re-deciding the schema at that point.
- All three old "IoT" stand-ins remain in place, untouched, for now.

## IoT ground-truth boundary — strict architectural rule (added — Task 1 correction pass, Correction 3)

`sensor_health` stays in `smartagri.model.IotSensorReading` for now, because it is part of the schema already approved (reconciliation blueprint §5). But its role must be bounded precisely, or a later task could accidentally wire it in as a shortcut:

**`sensor_health` is SIMULATOR/EVALUATION METADATA. It must NEVER be consumed by:**
- reliability estimation (the future `R_sat`/`R_iot` adaptive-reliability logic)
- Kalman fusion
- ML (feature engineering, training, or inference)
- conformal prediction
- the decision engine
- streaming state-management logic (e.g. `mapGroupsWithState`)

The only legitimate consumer of `sensor_health` is an **evaluation/experiment** step that compares what the fusion logic *inferred* about a sensor's reliability against what the simulator *actually did* — i.e. Experiments F–J in the reconciliation blueprint (§21/§15). Any component named above reading `sensor_health` is a bug, not a shortcut, because it would let the system "see the answer" instead of inferring reliability from the data itself, which is the entire point of the adaptive reliability design.

**Required separation in the future simulator** (not implemented in this task):

```
TRUE STATE
    |
    v
corruption process (noise / dropout / bias applied)
    |
    v
Kafka-observable sensor reading   <-- this is what IotSensorReading's
                                       observable fields represent

TRUE STATE + corruption configuration
    |
    v
evaluation-only ground truth      <-- this is what sensor_health represents,
                                       kept and used ONLY by the evaluation
                                       harness, never by the pipeline itself
```

Concretely: the future simulator generates a clean latent value and its corruption process separately from the observable Kafka payload, so `sensor_health` can be attached to a reading for later evaluation without that reading's *observable* fields (`soil_moisture_vwc`, `soil_temperature_c`, `air_temperature_c`) leaking any information about whether corruption was applied. `IotSensorReading` as currently modeled already keeps `sensor_health` as a separate field alongside the observable ones (never mixed into their values) — this section documents the rule; it does not change the model.

## SoilGrids duplication

## SoilGrids duplication

**OLD:** two separate Scala implementations —
1. `scala/soilgrids.scala` (legacy, spark-shell style), writing to `processed/soilgrids.parquet` — **the same path the Python extractor writes to**, a silent-overwrite risk
2. `scala-ingestion/src/main/scala/smartagri/SoilGridsIngestion.scala` (current sbt project), writing to `processed/soilgrids_scala.parquet`

**NEW:** `smartagri.model.SoilGridsObservation` — one canonical schema/model. Both existing Parquet outputs already agree on the same column names (`ph_water`, `nitrogen_g_kg`, `soil_organic_carbon`, `clay_pct`, `sand_pct`, `silt_pct`, `water_content_fc`), so no field renaming is needed — the model just formalizes the schema both already (mostly) produce.

- **Not fixed in this task.** `scala/soilgrids.scala` is untouched and still exists. A later ingestion-consolidation task should retire it (reconciliation blueprint §18/§25: `scala/soilgrids.scala` → **RETIRE**) so only `SoilGridsIngestion.scala` remains.

## IoT validation boundary (added — Task 1 correction pass, Correction 4)

`smartagri.validation.SchemaValidation.validateIotReading` exists today as a general-purpose validator, but it does not yet have a single, designated call site — that call site is a future task, not this one. Documenting the intended boundary now so it isn't decided ad hoc, differently, in each later task that touches IoT data:

```
raw event (JSON off the Kafka topic)
        |
        v
parse                    <-- deserialize JSON into raw field values
        |
        v
validate                 <-- SchemaValidation.validateIotReading (already built)
        |
        v
trusted IotSensorReading  <-- only past this point does a value get to be an
                               IotSensorReading anywhere else in the system
```

**This parse -> validate -> trusted boundary belongs to the future Kafka ingestion layer** (the Structured Streaming job's Kafka-source deserialization step, not yet implemented) — it happens exactly once, at the edge where untrusted external data enters the system. No downstream module (fusion, ML, conformal, decision engine) should re-validate or re-implement these checks; they should simply consume `IotSensorReading` values that are already known-valid by construction (the type cannot exist without having passed validation — see `docs/migration/legacy-compatibility-mapping.md`'s note on `CanonicalDate`/`IotEventTime`'s private-constructor pattern, which `IotSensorReading`'s nested `GridCell`/`IotEventTime` fields already benefit from today).

**Invalid records** (a message that fails `validateIotReading`) are explicitly NOT this task's concern to handle — no dead-letter topic, retry, or logging strategy is implemented here. That belongs to the future streaming error-handling design, and invalid records must not be silently coerced into a "best guess" trusted record in the meantime.

## Configuration

## Configuration

**OLD:** hard-coded path/constant literals scattered per module — `HdfsUtils.HDFS = "hdfs://namenode:9000"`; each of `SoilGridsIngestion`, `Era5Ingestion`, `SentinelIngestion`, `Analytics` redeclaring its own `RAW`/`OUT`/`PROC`/`RESULTS` string; `SentinelIngestion.GRID = 100`.

**NEW:** `smartagri.config.AppConfig`, with every default matching the existing hard-coded value exactly (see `AppConfig.scala`'s doc comment) — adopting it is backward compatible.

- **Not wired into the legacy modules in this task.** `HdfsUtils`, `SoilGridsIngestion`, `Era5Ingestion`, `SentinelIngestion`, and `Analytics` still use their own hard-coded constants today; `AppConfig` exists as the target structure for later tasks to actually read from, not yet as something the legacy modules call.

---

**Summary for later tasks:** nothing in this list has been fixed yet. This document is the map for *how* to fix each item consistently once the corresponding implementation task (ingestion migration, fusion rewrite, streaming build-out) actually happens, so each fix lands once, in the right place, using the canonical types defined in this task — rather than each future task re-deciding names and conventions independently.
