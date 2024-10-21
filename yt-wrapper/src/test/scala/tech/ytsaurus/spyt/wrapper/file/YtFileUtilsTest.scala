package tech.ytsaurus.spyt.wrapper.file

import org.apache.commons.io.IOUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.{GetFileFromCache, PutFileToCache, WriteFile}
import tech.ytsaurus.core.cypress.CypressNodeType
import tech.ytsaurus.spyt.test.{LocalYtClient, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt

class YtFileUtilsTest extends FlatSpec with Matchers with LocalYtClient with TmpDir {
  behavior of "YtCypressUtils"

  private val timeout = 10 minutes

  it should "upload a local file to the cache and read it from it" in {
    val remoteTempFilesDirectory = s"$tmpPath/yt_wrapper/file_storage"
    val fileToUploadPath = Paths.get(this.getClass.getResource("/file_to_upload").toURI).toString

    val ytClentSpy1: CompoundClient = Mockito.spy(yt)
    val cachePath = YtWrapper.uploadFileToCache(fileToUploadPath, timeout, remoteTempFilesDirectory)(ytClentSpy1)

    Mockito.verify(ytClentSpy1, times(1)).getFileFromCache(any(classOf[GetFileFromCache]))
    Mockito.verify(ytClentSpy1, times(1)).writeFile(any(classOf[WriteFile]))
    Mockito.verify(ytClentSpy1, times(1)).putFileToCache(any(classOf[PutFileToCache]))

    cachePath shouldEqual s"$remoteTempFilesDirectory/new_cache/28/7dfa64e383322d67682424244458ee28"

    val readFile = IOUtils.toString(YtWrapper.readFile(cachePath), Charset.defaultCharset())
    readFile shouldEqual "This is a simple file to use in YtFileUtils.uploadFileToCache"

    val ytClentSpy2: CompoundClient = Mockito.spy(yt)
    val cachePath2 = YtWrapper.uploadFileToCache(fileToUploadPath, timeout, remoteTempFilesDirectory)(ytClentSpy2)
    cachePath2 shouldEqual cachePath

    // checking that the file was not uploaded modified during second retrieval
    Mockito.verify(ytClentSpy2, times(1)).getFileFromCache(any(classOf[GetFileFromCache]))
    Mockito.verify(ytClentSpy2, times(0)).writeFile(any(classOf[WriteFile]))
    Mockito.verify(ytClentSpy2, times(0)).putFileToCache(any(classOf[PutFileToCache]))
  }
}
