package tech.ytsaurus.spyt.shuffle;

import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.client.AsyncWriter;
import tech.ytsaurus.client.rows.UnversionedRow;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class YTsaurusShuffleMapOutputWriterTest {
    private static final int SHUFFLE_ID = 1;
    private static final long MAP_ID = 3L;
    private static final int BUFFER_SIZE = 10;

    private final List<UnversionedRow> rows = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private final AsyncWriter<UnversionedRow> ytWriter = mock(AsyncWriter.class);

    @BeforeEach
    void setUp() {
        when(ytWriter.write(any())).thenAnswer(invocation -> {
            rows.addAll(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        when(ytWriter.finish()).thenReturn(CompletableFuture.completedFuture(null));
    }

    private YTsaurusShuffleMapOutputWriter writer(int numPartitions, int rowSize) {
        return new YTsaurusShuffleMapOutputWriter(ytWriter, SHUFFLE_ID, MAP_ID, numPartitions, rowSize, BUFFER_SIZE);
    }

    private static void writePartition(YTsaurusShuffleMapOutputWriter writer, int partition, String payload)
            throws IOException {
        final ShufflePartitionWriter partitionWriter = writer.getPartitionWriter(partition);
        try (OutputStream output = partitionWriter.openStream()) {
            output.write(payload.getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static String payloadOf(UnversionedRow row) {
        final byte[] value = row.getValues().get(1).bytesValue();
        assertEquals(MAP_ID, ByteBuffer.wrap(value, 0, Long.BYTES).getLong(), "row carries the map id");
        return new String(value, Long.BYTES, value.length - Long.BYTES, StandardCharsets.US_ASCII);
    }

    @Test
    void allocatesTheRowBufferOncePerMapTaskRatherThanOncePerPartition() throws IOException {
        final int numPartitions = 100;
        final int rowSize = 8 << 20;
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final long threadId = Thread.currentThread().getId();
        final long allocatedBefore = threads.getThreadAllocatedBytes(threadId);

        final YTsaurusShuffleMapOutputWriter writer = writer(numPartitions, rowSize);
        for (int partition = 0; partition < numPartitions; partition++) {
            writePartition(writer, partition, "p" + partition);
        }
        writer.commitAllPartitions(new long[0]);

        final long allocated = threads.getThreadAllocatedBytes(threadId) - allocatedBefore;
        // A buffer of the full row size per partition would take 100 x 8 MiB.
        assertTrue(allocated < 4L * rowSize, "allocated " + allocated + " bytes for " + numPartitions + " partitions");
    }

    @Test
    void writesEachPartitionIntoItsOwnRowsWithoutBytesOfThePreviousPartition() throws IOException {
        final YTsaurusShuffleMapOutputWriter writer = writer(3, 16);

        writePartition(writer, 0, "abcdefgh");
        writePartition(writer, 1, "");
        writePartition(writer, 2, "xy");
        final long[] lengths = writer.commitAllPartitions(new long[0]).getPartitionLengths();

        assertEquals(2, rows.size(), "an empty partition produces no row");
        assertEquals(0L, rows.get(0).getValues().get(0).longValue());
        assertEquals("abcdefgh", payloadOf(rows.get(0)));
        assertEquals(2L, rows.get(1).getValues().get(0).longValue());
        assertEquals("xy", payloadOf(rows.get(1)));
        assertArrayEquals(new long[]{8, 0, 2}, lengths);
    }

    @Test
    void refusesToOpenAPartitionStreamWhileAnotherOneIsOpen() throws IOException {
        final YTsaurusShuffleMapOutputWriter writer = writer(2, 16);
        final OutputStream first = writer.getPartitionWriter(0).openStream();
        first.write('a');

        // Both streams would write into the one row buffer of the map task.
        assertThrows(IllegalStateException.class, () -> writer.getPartitionWriter(1).openStream());

        first.close();
        writer.commitAllPartitions(new long[0]);
        assertEquals(1, rows.size());
        assertEquals("a", payloadOf(rows.get(0)));
    }

    @Test
    void repeatedCloseOfAPartitionStreamDoesNotFlushTheNextPartition() throws IOException {
        final YTsaurusShuffleMapOutputWriter writer = writer(2, 16);
        final OutputStream first = writer.getPartitionWriter(0).openStream();
        first.write('a');
        first.close();
        final OutputStream second = writer.getPartitionWriter(1).openStream();
        second.write('b');

        first.close();
        second.close();
        writer.commitAllPartitions(new long[0]);

        assertEquals(2, rows.size());
        assertEquals(0L, rows.get(0).getValues().get(0).longValue());
        assertEquals("a", payloadOf(rows.get(0)));
        assertEquals(1L, rows.get(1).getValues().get(0).longValue());
        assertEquals("b", payloadOf(rows.get(1)));
    }

    @Test
    void rejectsWritesThroughAClosedStreamWithoutTouchingTheNextPartition() throws IOException {
        final YTsaurusShuffleMapOutputWriter writer = writer(2, 16);
        final OutputStream first = writer.getPartitionWriter(0).openStream();
        first.write('a');
        first.close();
        final OutputStream second = writer.getPartitionWriter(1).openStream();
        second.write('b');

        // The closed stream shares the row buffer with the open one.
        assertThrows(IOException.class, () -> first.write('x'));
        assertThrows(IOException.class, () -> first.write(new byte[]{'y'}, 0, 1));
        second.close();
        final long[] lengths = writer.commitAllPartitions(new long[0]).getPartitionLengths();

        assertEquals(2, rows.size());
        assertEquals("a", payloadOf(rows.get(0)));
        assertEquals("b", payloadOf(rows.get(1)));
        assertArrayEquals(new long[]{1, 1}, lengths);
    }
}
