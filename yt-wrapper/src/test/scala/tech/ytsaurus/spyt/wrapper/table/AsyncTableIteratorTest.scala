package tech.ytsaurus.spyt.wrapper.table

import org.mockito.Mockito.{atLeastOnce, doNothing}
import org.mockito.MockitoSugar.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.AsyncReader

import java.util.concurrent.CompletableFuture

class AsyncTableIteratorTest extends AnyFlatSpec with Matchers {

  it should "iterate through all elements from AsyncReader" in {
    val mockReader = mock[AsyncReader[String]]
    val reportBytesRead = mock[Long => Unit]

    val batch1 = java.util.List.of("a", "b", "c")
    val batch2 = java.util.List.of("d", "e")
    val emptyBatch: Null = null

    when(mockReader.next()).thenReturn(
      CompletableFuture.completedFuture(batch1),
      CompletableFuture.completedFuture(batch2),
      CompletableFuture.completedFuture(emptyBatch)
    )

    doNothing().when(mockReader).close()

    val iterator = new AsyncTableIterator(mockReader, reportBytesRead)
    val result = scala.collection.mutable.ListBuffer[String]()
    while (iterator.hasNext) {
      result += iterator.next()
    }

    result should contain theSameElementsInOrderAs List("a", "b", "c", "d", "e")
    verify(mockReader).close()
  }

  it should "handle empty reader correctly" in {
    val mockReader = mock[AsyncReader[String]]
    val reportBytesRead = mock[Long => Unit]

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(null))
    doNothing().when(mockReader).close()

    val iterator = new AsyncTableIterator(mockReader, reportBytesRead)

    iterator.hasNext should be(false)
    iterator.hasNext should be(false) // Repeat call after closing reader

    verify(mockReader, atLeastOnce()).close()
  }

  it should "throw NoSuchElementException when calling next() on empty iterator" in {
    val mockReader = mock[AsyncReader[String]]
    val reportBytesRead = mock[Long => Unit]

    when(mockReader.next()).thenReturn(
      CompletableFuture.completedFuture(java.util.List.of[String]("a")),
      CompletableFuture.completedFuture(null)
    )
    doNothing().when(mockReader).close()

    val iterator = new AsyncTableIterator(mockReader, reportBytesRead)
    iterator.hasNext
    println(iterator.next())
    iterator.hasNext

    an[NoSuchElementException] should be thrownBy {
      iterator.next()
    }
  }

  it should "throw NullPointerException when calling next() on not initialized iterator" in {
    val mockReader = mock[AsyncReader[String]]
    val reportBytesRead = mock[Long => Unit]

    when(mockReader.next()).thenReturn(CompletableFuture.completedFuture(null))
    doNothing().when(mockReader).close()

    val iterator = new AsyncTableIterator(mockReader, reportBytesRead)

    an[NullPointerException] should be thrownBy {
      iterator.next()
    }
  }
}