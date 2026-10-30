package smartagri.preprocess

import java.awt.image.Raster
import java.io.ByteArrayInputStream
import javax.imageio.ImageReader
import javax.imageio.stream.MemoryCacheImageInputStream

import com.github.jaiimageio.jpeg2000.impl.J2KImageReaderSpi

/** Identity of one Sentinel-2 L2A product, parsed from its SAFE/zip file name.
  * e.g. S2A_MSIL2A_20230413T050651_N0510_R019_T43PHQ_20240829T072229.zip */
final case class Sentinel2ProductName(
    fileName: String,
    productId: String,
    mission: String,
    sensingDate: String,      // yyyy-MM-dd
    sensingTime: String,      // yyyyMMddTHHmmss
    processingBaseline: String,
    relativeOrbit: String,
    tile: String,             // e.g. 43PHQ (without the leading T)
    generationTime: String
)

object Sentinel2ProductName {
  private val Pattern =
    """(S2[ABC])_MSIL2A_(\d{8}T\d{6})_N(\d{4})_R(\d{3})_T(\d{2}[A-Z]{3})_(\d{8}T\d{6})""".r

  def parse(fileName: String): Option[Sentinel2ProductName] =
    Pattern.findFirstMatchIn(fileName).map { m =>
      val sensing = m.group(2)
      Sentinel2ProductName(
        fileName = fileName,
        productId = m.matched,
        mission = m.group(1),
        sensingDate = s"${sensing.substring(0, 4)}-${sensing.substring(4, 6)}-${sensing.substring(6, 8)}",
        sensingTime = sensing,
        processingBaseline = s"${m.group(3).substring(0, 2)}.${m.group(3).substring(2)}",
        relativeOrbit = m.group(4),
        tile = "T" + m.group(5),
        generationTime = m.group(6)
      )
    }
}

/** Radiometric + geometric metadata needed to turn digital numbers into georeferenced reflectance. */
final case class Sentinel2Metadata(
    epsg: String,
    ulx20: Double,
    uly20: Double,
    quantification: Double,
    offsetB04: Double,
    offsetB8A: Double,
    cloudyPixelPct: Option[Double]
)

object Sentinel2Metadata {
  private def tag(xml: String, name: String): Option[String] =
    s"""<$name(?:\\s[^>]*)?>([^<]*)</$name>""".r.findFirstMatchIn(xml).map(_.group(1).trim)

  /** `<BOA_ADD_OFFSET band_id="3">-1000</BOA_ADD_OFFSET>` -- band_id 3 = B04, 8 = B8A.
    * Products older than processing baseline 04.00 have no offset element (offset 0). */
  private def boaOffset(xml: String, bandId: Int): Double =
    s"""<BOA_ADD_OFFSET band_id="$bandId">([^<]*)</BOA_ADD_OFFSET>""".r
      .findFirstMatchIn(xml).map(_.group(1).trim.toDouble).getOrElse(0.0)

  def parse(productXml: String, tileXml: String): Sentinel2Metadata = {
    val geo20 = """(?s)<Geoposition resolution="20">(.*?)</Geoposition>""".r
      .findFirstMatchIn(tileXml).map(_.group(1))
      .getOrElse(throw new IllegalStateException("MTD_TL.xml has no 20 m Geoposition"))
    Sentinel2Metadata(
      epsg = tag(tileXml, "HORIZONTAL_CS_CODE").getOrElse(throw new IllegalStateException("no HORIZONTAL_CS_CODE")),
      ulx20 = tag(geo20, "ULX").get.toDouble,
      uly20 = tag(geo20, "ULY").get.toDouble,
      quantification = tag(productXml, "BOA_QUANTIFICATION_VALUE").map(_.toDouble).getOrElse(10000.0),
      offsetB04 = boaOffset(productXml, 3),
      offsetB8A = boaOffset(productXml, 8),
      cloudyPixelPct = tag(productXml, "Cloud_Coverage_Assessment").map(_.toDouble)
    )
  }
}

/** Pure-Java JPEG2000 decoding (JJ2000 via jai-imageio). The reader is instantiated
  * directly from its SPI class, so it works inside YARN executors without relying on
  * ImageIO's classpath plugin scan. */
object Jp2Decoder {

  /**
   * Returns the raw JPEG2000 codestream held in the JP2 file's `jp2c` box.
   *
   * Sentinel-2 JP2 files store the codestream in a box with a 64-bit extended length
   * (LBox == 1), which JJ2000's JP2 file-format parser rejects ("File too long."). The
   * box layout is simple (ISO 15444-1 Annex I), so it is walked here and JJ2000 is given
   * the bare codestream instead. Input that is already a bare codestream (SOC marker
   * FF4F) is returned unchanged.
   */
  def extractCodestream(bytes: Array[Byte]): Array[Byte] = {
    if (bytes.length > 1 && (bytes(0) & 0xff) == 0xff && (bytes(1) & 0xff) == 0x4f) return bytes
    val buf = java.nio.ByteBuffer.wrap(bytes)
    var pos = 0
    while (pos + 8 <= bytes.length) {
      val lbox = buf.getInt(pos) & 0xffffffffL
      val tbox = new String(bytes, pos + 4, 4, "US-ASCII")
      val (header, boxLen) =
        if (lbox == 1L) (16, buf.getLong(pos + 8))
        else if (lbox == 0L) (8, (bytes.length - pos).toLong)
        else (8, lbox)
      if (tbox == "jp2c") return java.util.Arrays.copyOfRange(bytes, pos + header, (pos + boxLen).toInt)
      if (boxLen < header) throw new IllegalStateException(s"corrupt JP2 box '$tbox' at $pos")
      pos += boxLen.toInt
    }
    throw new IllegalStateException("no jp2c (codestream) box in JP2 file")
  }

  def decode(bytes: Array[Byte]): Raster = {
    val reader: ImageReader = new J2KImageReaderSpi().createReaderInstance()
    val stream = new MemoryCacheImageInputStream(new ByteArrayInputStream(extractCodestream(bytes)))
    try {
      reader.setInput(stream, true, true)
      reader.read(0).getRaster
    } finally {
      reader.dispose()
      stream.close()
    }
  }
}

/** Sentinel-2 L2A Scene Classification (SCL) classes used for pixel QA. */
object SceneClass {
  val NoData = 0; val Saturated = 1; val DarkArea = 2; val CloudShadow = 3; val Vegetation = 4
  val NotVegetated = 5; val Water = 6; val Unclassified = 7; val CloudMedium = 8
  val CloudHigh = 9; val ThinCirrus = 10; val Snow = 11

  /** Pixels whose reflectance describes the land surface (usable for NDVI). */
  def isClearLand(c: Int): Boolean = c == Vegetation || c == NotVegetated || c == Unclassified
  def isCloud(c: Int): Boolean = c == CloudShadow || c == CloudMedium || c == CloudHigh || c == ThinCirrus
  def isNoData(c: Int): Boolean = c == NoData || c == Saturated
}
