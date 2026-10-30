package smartagri

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.io.IOUtils
import java.net.URI

/** Shared helpers for reading raw bytes out of HDFS. */
object HdfsUtils {
  val HDFS = "hdfs://namenode:9000"

  def hdfs(conf: Configuration): FileSystem =
    FileSystem.get(new URI(HDFS), conf)

  def readBytes(fs: FileSystem, path: Path): Array[Byte] = {
    val in = fs.open(path)
    try IOUtils.toByteArray(in) finally in.close()
  }

  /** Copy an HDFS file to a local temp file. Caller is responsible for deletion. */
  def toLocalTemp(fs: FileSystem, path: Path, suffix: String): String = {
    val tmp = java.io.File.createTempFile("ingest_", suffix)
    // NOTE: no deleteOnExit() — causes IllegalStateException during spark-shell
    // shutdown. The caller's finally block handles deletion.
    val in = fs.open(path)
    val out = new java.io.FileOutputStream(tmp)
    try IOUtils.copy(in, out) finally { in.close(); out.close() }
    tmp.getAbsolutePath
  }
}