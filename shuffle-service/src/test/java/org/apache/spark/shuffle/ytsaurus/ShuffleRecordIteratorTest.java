package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.client.AsyncReader;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ShuffleRecordIteratorTest {
    private static UnversionedRow row(long mapId, String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = ByteBuffer.allocate(Long.BYTES + data.length).putLong(mapId).put(data).array();
        return new UnversionedRow(List.of(new UnversionedValue(0, ColumnValueType.INT64, false, 0L),
                new UnversionedValue(1, ColumnValueType.STRING, false, bytes)));
    }

    @SafeVarargs
    @SuppressWarnings("unchecked")
    private static AsyncReader<UnversionedRow> reader(List<UnversionedRow>... batches) {
        AsyncReader<UnversionedRow> reader = mock(AsyncReader.class);
        List<List<UnversionedRow>> remaining = new ArrayList<>(List.of(batches));
        when(reader.next()).thenAnswer(invocation -> CompletableFuture.completedFuture(
                remaining.isEmpty() ? null : remaining.remove(0)));
        return reader;
    }

    private static Iterator<Tuple2<Integer, String>> deserialize(InputStream stream) {
        try {
            String value = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            stream.close();
            List<Tuple2<Integer, String>> records = value.isEmpty()
                    ? Collections.emptyList() : Collections.singletonList(new Tuple2<>(0, value));
            return JavaConverters.asScalaIteratorConverter(records.iterator()).asScala();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void joinsRowsByMapAcrossBatchesAndSkipsEmptyPartitions() {
        ShuffleReadMetricsReporter metrics = mock(ShuffleReadMetricsReporter.class);
        List<AsyncReader<UnversionedRow>> readers = List.of(reader(),
                reader(List.of(row(10, "ab")), List.of(), List.of(row(10, "cd"), row(11, "ef"))),
                reader(List.of()), reader(List.of(row(12, "gh"))));
        List<Integer> requested = new ArrayList<>();
        ShuffleRecordIterator<Integer, String> records = new ShuffleRecordIterator<>(3, 7, partition -> {
            requested.add((Integer) partition);
            return CompletableFuture.completedFuture(readers.get((Integer) partition - 3));
        }, ShuffleRecordIteratorTest::deserialize, metrics);
        List<String> result = new ArrayList<>();
        while (records.hasNext()) {
            assertTrue(records.hasNext());
            result.add(records.next()._2());
        }
        assertFalse(records.hasNext());
        assertEquals(List.of("abcd", "ef", "gh"), result);
        assertEquals(List.of(3, 4, 5, 6), requested);
        verify(metrics, times(4)).incRemoteBytesRead(10);
    }

    @Test
    void skipsEmptyDeserializedStreamsBeforeBetweenAndAfterRecords() {
        AsyncReader<UnversionedRow> reader = reader(List.of(row(1, ""), row(2, ""), row(3, "first"), row(4, ""),
                row(5, ""), row(6, "second"), row(7, ""), row(8, "")));
        ShuffleRecordIterator<Integer, String> records = new ShuffleRecordIterator<>(
                0, 1, partition -> CompletableFuture.completedFuture(reader),
                ShuffleRecordIteratorTest::deserialize, mock(ShuffleReadMetricsReporter.class));

        assertTrue(records.hasNext());
        assertTrue(records.hasNext());
        assertEquals("first", records.next()._2());
        assertTrue(records.hasNext());
        assertTrue(records.hasNext());
        assertEquals("second", records.next()._2());
        assertFalse(records.hasNext());
        assertFalse(records.hasNext());
        assertThrows(java.util.NoSuchElementException.class, records::next);
    }

    @Test
    void exhaustsAllEmptyDeserializedStreams() {
        List<AsyncReader<UnversionedRow>> readers = List.of(reader(List.of(row(1, ""), row(2, ""))),
                reader(List.of(row(3, ""))));
        List<Integer> requested = new ArrayList<>();
        ShuffleRecordIterator<Integer, String> records = new ShuffleRecordIterator<>(0, 2, partition -> {
            requested.add((Integer) partition);
            return CompletableFuture.completedFuture(readers.get((Integer) partition));
        }, ShuffleRecordIteratorTest::deserialize, mock(ShuffleReadMetricsReporter.class));

        assertFalse(records.hasNext());
        assertEquals(List.of(0, 1), requested);
        assertFalse(records.hasNext());
        assertThrows(java.util.NoSuchElementException.class, records::next);
    }

    @Test
    void emptyRangeDoesNotRequestReaders() {
        ShuffleRecordIterator<Integer, String> records = new ShuffleRecordIterator<>(
                2, 2, partition -> { throw new AssertionError("Unexpected reader request"); },
                ShuffleRecordIteratorTest::deserialize, mock(ShuffleReadMetricsReporter.class));
        assertFalse(records.hasNext());
        assertFalse(records.hasNext());
        assertThrows(java.util.NoSuchElementException.class, records::next);
    }

    @Test
    void propagatesReaderCreationAndBatchFailures() {
        RuntimeException failure = new IllegalStateException("reader failure");
        ShuffleRecordIterator<Integer, String> creationFailure = new ShuffleRecordIterator<>(
                0, 1, partition -> CompletableFuture.failedFuture(failure),
                ShuffleRecordIteratorTest::deserialize, mock(ShuffleReadMetricsReporter.class));
        assertSame(failure, assertThrows(CompletionException.class, creationFailure::hasNext).getCause());
        AsyncReader<UnversionedRow> reader = reader();
        when(reader.next()).thenReturn(CompletableFuture.failedFuture(failure));
        ShuffleRecordIterator<Integer, String> batchFailure = new ShuffleRecordIterator<>(
                0, 1, partition -> CompletableFuture.completedFuture(reader),
                ShuffleRecordIteratorTest::deserialize, mock(ShuffleReadMetricsReporter.class));
        assertSame(failure, assertThrows(CompletionException.class, batchFailure::hasNext).getCause());
    }

    @Test
    void propagatesDeserializationFailure() {
        RuntimeException failure = new IllegalArgumentException("invalid payload");
        ShuffleRecordIterator<Integer, String> records = new ShuffleRecordIterator<>(
                0, 1, partition -> CompletableFuture.completedFuture(reader(List.of(row(1, "bad")))),
                stream -> { throw failure; }, mock(ShuffleReadMetricsReporter.class));
        assertSame(failure, assertThrows(IllegalArgumentException.class, records::hasNext));
    }
}
