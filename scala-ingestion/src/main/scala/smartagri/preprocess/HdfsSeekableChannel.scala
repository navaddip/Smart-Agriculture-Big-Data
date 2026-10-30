package smartagri.preprocess

import java.nio.ByteBuffer
import java.nio.channels.{NonWritableChannelException, SeekableByteChannel}

import org.apache.hadoop.fs.FSDataInputStream

/**
 * Random-access view of an HDFS file, so a zip archive can be opened in place.
 *
 * A Sentinel-2 L2A product is a ~1.2 GB zip, but NDVI preprocessing needs only
 * three 20 m rasters (~70 MB). Opening the zip through this channel means an
 * executor reads the zip central directory plus exactly those entries via
 * HDFS positional reads, instead of streaming (or copying) the whole archive.
 * `bytesRead` is reported per product as I/O evidence.
 */
final class HdfsSeekableChannel(in: FSDataInputStream, fileSize: Long) extends SeekableByteChannel {
  private var pos: Long = 0L
  private var open: Boolean = true
  var bytesRead: Long = 0L

  override def read(dst: ByteBuffer): Int = {
    if (pos >= fileSize) return -1
    val n = math.min(dst.remaining().toLong, fileSize - pos).toInt
    if (n == 0) return 0
    if (dst.hasArray) {
      in.readFully(pos, dst.array(), dst.arrayOffset() + dst.position(), n)
      dst.position(dst.position() + n)
    } else {
      val tmp = new Array[Byte](n)
      in.readFully(pos, tmp, 0, n)
      dst.put(tmp)
    }
    pos += n
    bytesRead += n
    n
  }

  override def write(src: ByteBuffer): Int = throw new NonWritableChannelException
  override def position(): Long = pos
  override def position(newPosition: Long): SeekableByteChannel = { pos = newPosition; this }
  override def size(): Long = fileSize
  override def truncate(size: Long): SeekableByteChannel = throw new NonWritableChannelException
  override def isOpen: Boolean = open
  override def close(): Unit = { open = false; in.close() }
}
