package tech.ytsaurus.spyt.test

import java.util.UUID

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, TestSuite}
import tech.ytsaurus.spyt.wrapper.YtWrapper

trait TmpDir extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: TestSuite with LocalYt =>

  def testDir: String = s"//tmp/test-${self.getClass.getCanonicalName}"
  val tmpPath = s"$testDir/test-${UUID.randomUUID()}"
  val hadoopTmpPath = s"ytTable:${tmpPath.drop(1)}"


  override def beforeAll(): Unit = {
    super.beforeAll()
    YtWrapper.removeDir(testDir, recursive = true, force = true)
    YtWrapper.createDir(testDir)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    YtWrapper.remove(tmpPath, force = true)
  }

  override def afterEach(): Unit = {
    YtWrapper.remove(tmpPath, force = true)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    YtWrapper.removeDir(testDir, recursive = true, force = true)
    super.afterAll()
  }
}
