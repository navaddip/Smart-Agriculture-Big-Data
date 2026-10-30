package smartagri
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem,Path}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import java.util.zip.ZipFile
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
object SentinelIngestion {
  import HdfsUtils.HDFS
  val RAW=s"$HDFS/user/smart_agri/raw/sentinel2"
  val OUT=s"$HDFS/user/smart_agri/processed/sentinel2_ndvi_scala.parquet"
  val GRID=100; val BOA= -1000.0
  def parseName(n:String):(String,String)={val t="_(T\\d{2}[A-Z]{3})_".r.findFirstMatchIn(n).map(_.group(1)).getOrElse("unknown");val d="_(\\d{8})T".r.findFirstMatchIn(n).map(_.group(1)).getOrElse("unknown");val dd=if(d!="unknown")s"${d.substring(0,4)}-${d.substring(4,6)}-${d.substring(6,8)}"else d;(t,dd)}
  def extract(zf:ZipFile,sfx:String):Option[java.io.File]=zf.entries.asScala.find(_.getName.endsWith(sfx)).map{e=>val f=java.io.File.createTempFile("band_",".jp2");val i=zf.getInputStream(e);val o=new java.io.FileOutputStream(f);try org.apache.commons.io.IOUtils.copy(i,o) finally{i.close();o.close()};f}
  def readBand(f:java.io.File):Array[Double]={val t=SinglebandGeoTiff(f.getAbsolutePath);val a=t.tile.toArrayDouble();f.delete();a}
  def run(spark:SparkSession):Unit={
    val fs=HdfsUtils.hdfs(spark.sparkContext.hadoopConfiguration)
    val zips=fs.listStatus(new Path(RAW)).filter(_.getPath.getName.endsWith(".zip"))
    println(s"Found ${zips.length} products")
    val rows=new ArrayBuffer[Row]()
    var idx=0
    for(st<-zips){idx+=1;val name=st.getPath.getName;val(tile,date)=parseName(name);println(s"[$idx/${zips.length}] $tile $date");var lz:String=null
      try{lz=HdfsUtils.toLocalTemp(fs,st.getPath,".zip");val zf=new ZipFile(lz);val b4=extract(zf,"B04_10m.jp2");val b8=extract(zf,"B08_10m.jp2");zf.close()
        if(b4.isEmpty||b8.isEmpty)println("  bands missing")
        else{val red=readBand(b4.get);val nir=readBand(b8.get);val full=math.sqrt(red.length.toDouble).toInt;val step=full/GRID
          if(step>0){var gr=0;while(gr<GRID){var gc=0;while(gc<GRID){var sum=0.0;var cnt=0;var r0=gr*step;val r1=(gr+1)*step
            while(r0<r1){var c0=gc*step;val c1=(gc+1)*step;while(c0<c1){val i=r0*full+c0;if(i<red.length&&i<nir.length){val rd=red(i);val nr=nir(i);if(rd!=0.0&&nr!=0.0){val r=rd+BOA;val n=nr+BOA;val d=n+r;if(d!=0.0){val v=(n-r)/d;if(v>= -1.0&&v<=1.0){sum+=v;cnt+=1}}}};c0+=1};r0+=1}
            if(cnt>0)rows+=Row(tile,date,gr,gc,sum/cnt);gc+=1};gr+=1};println(s"  ok rows=${rows.length}")}}}
      catch{case e:Throwable=>println(s"  ERROR $name: ${e.getClass.getSimpleName}: ${e.getMessage}")}
      finally{if(lz!=null)new java.io.File(lz).delete()}}
    val schema=StructType(Seq(StructField("tile",StringType),StructField("date",StringType),StructField("cell_row",IntegerType),StructField("cell_col",IntegerType),StructField("ndvi",DoubleType)))
    val df=spark.createDataFrame(spark.sparkContext.parallelize(rows),schema)
    df.write.mode("overwrite").parquet(OUT);println(s"Wrote ${df.count()} rows -> $OUT")}
  def main(args:Array[String]):Unit={val spark=SparkSession.builder().appName("SentinelIngestion").getOrCreate();run(spark);spark.stop()}}

