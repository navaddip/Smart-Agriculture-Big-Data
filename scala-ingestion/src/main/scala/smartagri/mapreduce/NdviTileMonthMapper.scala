package smartagri.mapreduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
 * Classic Hadoop `Mapper` for the NDVI tile-month aggregation job.
 *
 * A thin wrapper around `NdviRecordParser`'s pure parsing/classification
 * logic -- this class's only responsibility is satisfying the genuine
 * Hadoop `Mapper` contract: read `(LongWritable, Text)` input records
 * (one per line of the Task 2A flat export, via `TextInputFormat`) and
 * emit `(Text, NdviObservationWritable)` keyed by `"tile|year_month"`
 * (Task 2B: cell_row/cell_col are deliberately excluded from the
 * grouping key -- this job aggregates at tile-month granularity, not
 * per-cell).
 *
 * Malformed records (Task 2B requirement 2) are counted via a Hadoop
 * `Counter` and skipped, never silently treated as any NDVI
 * classification and never allowed to throw out of `map` -- one bad
 * line must not fail an entire distributed job over hundreds of
 * thousands of records. The counter is the durable, job-level evidence
 * of how many records were rejected, visible in the job's own reported
 * counters after a run (Task 2F's evidence-gathering step, not this
 * task, will make use of it).
 */
class NdviTileMonthMapper extends Mapper[LongWritable, Text, Text, NdviObservationWritable] {

  private val outKey = new Text()

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, NdviObservationWritable]#Context
  ): Unit = {
    NdviRecordParser.parseLine(value.toString) match {
      case NdviRecordParser.Parsed(record) =>
        outKey.set(NdviRecordParser.groupingKey(record.tile, record.yearMonth))
        context.write(outKey, record.observation)

      case NdviRecordParser.Malformed(_) =>
        context.getCounter("NdviTileMonthJob", "MALFORMED_RECORDS").increment(1L)
    }
  }
}
