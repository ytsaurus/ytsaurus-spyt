package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.test.{LocalSpark, LocalYtClient, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt._

import java.io.FileNotFoundException
import java.net.URI

class YtTableFileSystemTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  behavior of "YtTableFileSystem"

  val fs = new YtTableFileSystem()
  fs.initialize(URI.create("ytTable:/"), fsConf)

  private def setupData(): Unit = {
    YtWrapper.createDir(tmpPath)
    val schema = TableSchema.builder().addValue("a", ColumnValueType.INT64).build()
    (1 to 100).foreach { n =>
      val path = s"$tmpPath/%04d".format(n)
      writeTableFromYson(Seq(s"{a = $n}"), path, schema)
    }
  }

  it should "recursive list directory with 100 nested tables using a single request to cypress" in {
    setupData()
    val result = fs.listStatus(new Path(tmpPath))
    result should have size 100
  }

  it should "process a list of paths in a single request" in {
    setupData()
    import spark.implicits._
    val paths = (30 to 45).map { n => s"$tmpPath/%04d".format(n)}

    val result = spark.read.yt(paths: _*).as[Long].collect()
    result should contain theSameElementsAs (30L to 45L)
  }

  private def absenceTest(invocation: Path => Unit): Unit = {
    val path = "//some/absent/entity"
    try {
      invocation(new Path(path))
    } catch {
      case fnf: FileNotFoundException =>
        fnf.getMessage should include(path)
    }
  }

  it should "correctly deal with absent directory" in {
    absenceTest(p => fs.listStatus(p))
  }

  it should "correctly deal with absent file" in {
    absenceTest(fs.getFileStatus)
  }

}
