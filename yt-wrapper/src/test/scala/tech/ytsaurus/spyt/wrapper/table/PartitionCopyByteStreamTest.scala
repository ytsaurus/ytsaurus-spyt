package tech.ytsaurus.spyt.wrapper.table

import org.mockito.Mockito.{atLeastOnce, doNothing, never}
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.AsyncReader

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

class PartitionCopyByteStreamTest extends AnyFlatSpec with Matchers {

  it should "read single batch successfully" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5))
    val batchList = java.util.List.of[ByteBuffer](data)

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(batchList))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)
    val buffer = new Array[Byte](5)
    val bytesRead = stream.read(buffer)

    bytesRead should be(5)
    buffer should contain theSameElementsAs Array[Byte](1, 2, 3, 4, 5)

    verify(reportBytesRead).apply(5L)
    verify(mockReader, never()).close()
  }

  it should "handle empty batch list" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(null))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)
    val buffer = new Array[Byte](5)
    val bytesRead = stream.read(buffer)

    bytesRead should be(0)
  }

  it should "handle empty batch" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val emptyList = java.util.List.of[ByteBuffer]()
    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(emptyList))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)

    val buffer = new Array[Byte](5)
    val bytesRead = stream.read(buffer)

    bytesRead should be(0)
  }

  it should "read multiple batches" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val batch1 = ByteBuffer.wrap(Array[Byte](1, 2, 3))
    val batch2 = ByteBuffer.wrap(Array[Byte](4, 5, 6))

    when(mockReader.next()).thenReturn(
      CompletableFuture.completedFuture(java.util.List.of[ByteBuffer](batch1)),
      CompletableFuture.completedFuture(java.util.List.of[ByteBuffer](batch2)),
      CompletableFuture.completedFuture(null)
    )
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)

    val buffer1 = new Array[Byte](3)
    stream.read(buffer1) should be(3)
    buffer1 should contain theSameElementsAs Array[Byte](1, 2, 3)

    val buffer2 = new Array[Byte](3)
    stream.read(buffer2) should be(3)
    buffer2 should contain theSameElementsAs Array[Byte](4, 5, 6)

    val buffer3 = new Array[Byte](3)
    stream.read(buffer3) should be(0)

    verify(reportBytesRead, atLeastOnce()).apply(3L)
    verify(reportBytesRead, atLeastOnce()).apply(3L)
  }

  it should "close reader properly" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val data = ByteBuffer.wrap(Array[Byte](1, 2, 3))
    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(java.util.List.of[ByteBuffer](data)))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)
    val buffer = new Array[Byte](3)
    stream.read(buffer)

    stream.close()
    verify(mockReader).close()
  }

  it should "handle nextPageToken correctly" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val token = Array(-1, -1, -1, -1, 0, 0, 0, 0).map(_.toByte)
    val data = Array[Byte](1, 2, 3)
    val buffer = ByteBuffer.allocate(token.length + data.length)
    buffer.put(token)
    buffer.put(data)
    buffer.flip()

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(java.util.List.of[ByteBuffer](buffer)))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)

    stream.isNextPage should be(true)

    val readBuffer = new Array[Byte](3)
    stream.read(readBuffer) should be(3)
    readBuffer should contain theSameElementsAs data
  }

  it should "handle emptySchemaToken correctly" in {
    val mockReader = mock[AsyncReader[ByteBuffer]]
    val reportBytesRead = mock[Long => Unit]

    val tokenBytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
    val buffer = ByteBuffer.wrap(tokenBytes)

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(java.util.List.of[ByteBuffer](buffer)))
    doNothing().when(mockReader).close()

    val stream = new PartitionCopyByteStream(mockReader, reportBytesRead)
    stream.isEmptyPage should be(true)
  }

  it should "fail when reader is null" in {
    val reportBytesRead = mock[Long => Unit]
    val stream = new PartitionCopyByteStream(null, reportBytesRead)
    val readBuffer = new Array[Byte](3)
    a[NullPointerException] should be thrownBy {
      stream.read(readBuffer)
    }
  }
}