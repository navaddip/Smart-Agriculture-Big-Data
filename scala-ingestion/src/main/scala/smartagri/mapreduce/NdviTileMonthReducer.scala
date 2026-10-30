package smartagri.mapreduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * Classic Hadoop `Reducer` for the NDVI tile-month aggregation job.
 *
 * A thin wrapper around `NdviTileMonthAggregator`'s pure, O(1)-memory
 * accumulation logic -- this class's only responsibility is satisfying
 * the genuine Hadoop `Reducer` contract: for each `"tile|year_month"`
 * key, fold its `Iterable[NdviObservationWritable]` through exactly ONE
 * `NdviTileMonthAggregator` in a single pass (never `.toList`/`.toSeq`/
 * `.toArray`/`collect()`-ing the values, per Task 2B's explicit
 * requirement), then emit one output record.
 *
 * Output key/value: `(Text(tile), Text("year_month\t<9 stat fields>"))`.
 * Hadoop's `TextOutputFormat` writes `key<sep>value` per line with a
 * default `<sep>` of a tab -- so `Text(tile)` as the key and
 * `Text("year_month\tobservation_count\t...\tquality_rate")` as the
 * value together produce exactly the eleven-tab-delimited-column output
 * line Task 2B specifies (`tile, year_month, observation_count,
 * valid_count, invalid_count, missing_count, mean_ndvi, min_ndvi,
 * max_ndvi, stddev_ndvi, quality_rate`), without needing a custom
 * `RecordWriter`.
 */
class NdviTileMonthReducer extends Reducer[Text, NdviObservationWritable, Text, Text] {

  private val outKey = new Text()
  private val outValue = new Text()

  override def reduce(
      key: Text,
      values: java.lang.Iterable[NdviObservationWritable],
      context: Reducer[Text, NdviObservationWritable, Text, Text]#Context
  ): Unit = {
    val keyString = key.toString
    // lastIndexOf, not indexOf: year_month is always exactly "YYYY-MM" (never contains '|'), while
    // tile is now guaranteed pipe-free by NdviRecordParser.parseLine's explicit guard (Task 2C audit
    // finding) -- splitting on the LAST separator is correct either way and is strictly safer
    // defense-in-depth than splitting on the first, in case that upstream guard is ever bypassed.
    val separatorIndex = keyString.lastIndexOf(NdviRecordParser.GroupingKeySeparator)

    if (separatorIndex < 0) {
      // Unreachable given the Mapper always emits via NdviRecordParser.groupingKey (which always
      // includes '|'), but handled explicitly rather than throwing, consistent with this job's
      // "reject, don't crash" posture toward unexpected input (see the Mapper's malformed-record
      // handling) -- a corrupted intermediate key must not fail the whole reduce phase.
      context.getCounter("NdviTileMonthJob", "MALFORMED_GROUP_KEYS").increment(1L)
    } else {
      val tile = keyString.substring(0, separatorIndex)
      val yearMonth = keyString.substring(separatorIndex + 1)

      val aggregator = new NdviTileMonthAggregator()
      val iterator = values.iterator()
      while (iterator.hasNext) {
        aggregator.accumulate(iterator.next())
      }
      val summary = aggregator.summary()

      outKey.set(tile)
      outValue.set((yearMonth +: summary.toTabDelimitedFields).mkString("\t"))
      context.write(outKey, outValue)
    }
  }
}
