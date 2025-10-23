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

  private def setupData(filenames: Seq[String]): Unit = {
    YtWrapper.createDir(tmpPath)
    val schema = TableSchema.builder().addValue("a", ColumnValueType.STRING).build()
    filenames.foreach { filename =>
      val slashIndex = filename.lastIndexOf("/")
      if (slashIndex > 0) {
        YtWrapper.createDir(s"$tmpPath/${filename.substring(0, slashIndex)}", None, ignoreExisting = true)
      }
      val path = s"$tmpPath/$filename"
      writeTableFromYson(Seq(s"""{a = "$filename"}"""), path, schema)
    }
  }

  private val oneTo100 = (1 to 100).map(n => "%04d".format(n))

  it should "recursive list directory with 100 nested tables using a single request to cypress" in {
    setupData(oneTo100)
    val result = fs.listStatus(new Path(tmpPath))
    result should have size 100
  }

  it should "process a list of paths in a single request" in {
    setupData(oneTo100)
    import spark.implicits._
    val paths = (30 to 45).map { n => s"$tmpPath/%04d".format(n)}

    val result = spark.read.yt(paths: _*).as[String].collect().map(_.toInt)
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

  it should "correctly process directories with non-standard symbol filenames" in {
    val names = List(
      "10817_15319_merged",
      "11298_15818_merged",
      "11650_16166_2025-07-23T00:00:00"
    )
    setupData(names)
    import spark.implicits._

    (0 to 1).foreach { offset =>
      val namesSublist = names.slice(offset, offset + 2)
      val paths = namesSublist.map(name => s"$tmpPath/$name")

      val result = spark.read.yt(paths: _*).as[String].collect()
      result should contain theSameElementsAs namesSublist
    }
  }

  it should "deal with long lists from different directories" in withConf(
    "spark.yt.read.listParentDirectories", "false"
  ) {
    val files = (1 to 10).flatMap { parent =>
      (1 to 10).map { child =>
        s"parent_$parent/child_$child"
      }
    }
    setupData(files)
    val namesToRead = (1 to 10).map(n => s"parent_$n/child_$n")
    val paths = namesToRead.map(name => s"$tmpPath/$name")
    import spark.implicits._

    val result = spark.read.yt(paths: _*).as[String].collect()
    result should contain theSameElementsAs namesToRead
  }
}
