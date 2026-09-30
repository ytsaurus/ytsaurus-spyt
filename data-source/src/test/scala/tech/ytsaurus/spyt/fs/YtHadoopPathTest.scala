package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import tech.ytsaurus.spyt.fs.path.YPathEnriched

class YtHadoopPathTest extends AnyFlatSpec with Matchers {

  it should "preserve security tags when Spark serializes file statuses as paths" in {
    val metadata = YtTableMeta(securityTags = Seq("tag_with_underscores", "tag/with/slashes", "données", "a b"))
    val original = YtHadoopPath(YPathEnriched.fromString("//tmp/input"), metadata)
    val restored = YtHadoopPath.fromPath(new Path(original.toString)).asInstanceOf[YtHadoopPath]
    restored.ypath shouldBe original.ypath
    restored.meta shouldBe metadata
  }

  it should "read paths created before security tag metadata was added" in {
    val path = new Path("ytTable:/tmp/input/4_16_0_scan_false_true_None")
    val restored = YtHadoopPath.fromPath(path).asInstanceOf[YtHadoopPath]
    restored.meta shouldBe YtTableMeta(rowCount = 4, size = 16)
  }
}
