package tech.ytsaurus.spyt.shuffle;

import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.client.AsyncWriter;
import tech.ytsaurus.client.rows.UnversionedRow;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

class YTsaurusSingleSpillMapOutputWriterTest {
    private static final int SHUFFLE_ID = 1;
    private static final long MAP_ID = 3L;
    // Room for the 8 byte map id header and 8 payload bytes.
    private static final int ROW_SIZE = 16;
    private static final int BUFFER_SIZE = 10;

    private final List<UnversionedRow> rows = new ArrayList<>();
    @SuppressWarnings("unchecked")
    private final AsyncWriter<UnversionedRow> ytWriter = mock(AsyncWriter.class);
    private final ShuffleWriteMetricsReporter writeMetrics = mock(ShuffleWriteMetricsReporter.class);
    private final YTsaurusSingleSpillMapOutputWriter writer = new YTsaurusSingleSpillMapOutputWriter(
            numPartitions -> new YTsaurusShuffleMapOutputWriter(ytWriter, SHUFFLE_ID, MAP_ID, numPartitions, ROW_SIZE,
                    BUFFER_SIZE),
            ROW_SIZE,
            writeMetrics);

    @BeforeEach
    void setUp() {
        when(ytWriter.write(any())).thenAnswer(invocation -> {
            rows.addAll(invocation.getArgument(0));
            return CompletableFuture.completedFuture(null);
        });
        when(ytWriter.finish()).thenReturn(CompletableFuture.completedFuture(null));
    }

    private static File spillFile(byte[] content) throws IOException {
        final File file = Files.createTempFile("spill", ".data").toFile();
        file.deleteOnExit();
        Files.write(file.toPath(), content);
        return file;
    }

    private static File spillFile(String content) throws IOException {
        return spillFile(content.getBytes(StandardCharsets.US_ASCII));
    }

    /** Concatenates the payloads of the captured rows, dropping the map id header each row carries. */
    private byte[] payloadOfCapturedRows() throws IOException {
        final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (UnversionedRow row : rows) {
            final byte[] value = row.getValues().get(1).bytesValue();
            assertEquals(MAP_ID, ByteBuffer.wrap(value, 0, Long.BYTES).getLong(), "row carries the map id");
            payload.write(value, Long.BYTES, value.length - Long.BYTES);
        }
        return payload.toByteArray();
    }

    @Test
    void doesNotCountTransferredBytesAsShuffleBytesWritten() throws IOException {
        writer.transferMapSpillFile(spillFile("abcdefg"), new long[]{2, 5}, new long[0]);

        // Spark counted these bytes when the spill file was produced; counting them here doubles the metric.
        verify(writeMetrics, never()).incBytesWritten(anyLong());
        verify(writeMetrics, atLeastOnce()).incWriteTime(anyLong());
    }

    @Test
    void transfersEveryPartitionInOrderWithItsMapIdHeader() throws IOException {
        writer.transferMapSpillFile(spillFile("abcdefg"), new long[]{2, 0, 5}, new long[0]);

        assertEquals(2, rows.size(), "an empty partition produces no row");
        assertEquals(0L, rows.get(0).getValues().get(0).longValue());
        assertEquals(2L, rows.get(1).getValues().get(0).longValue());
        assertArrayEquals("abcdefg".getBytes(StandardCharsets.US_ASCII), payloadOfCapturedRows());
        verify(ytWriter).finish();
    }

    @Test
    void splitsAPartitionLongerThanOneRowIntoRowsOfRowSize() throws IOException {
        final byte[] content = new byte[20];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }

        writer.transferMapSpillFile(spillFile(content), new long[]{content.length}, new long[0]);

        assertEquals(3, rows.size(), "20 bytes take three rows of at most 8 payload bytes");
        for (UnversionedRow row : rows) {
            assertTrue(row.getValues().get(1).bytesValue().length <= ROW_SIZE, "a row fits the row size");
        }
        assertArrayEquals(content, payloadOfCapturedRows());
    }

    @Test
    void failsAndAbortsWhenSpillFileIsShorterThanPartitionLengths() throws IOException {
        final File spill = spillFile("abc");

        final IOException failure = assertThrows(IOException.class,
                () -> writer.transferMapSpillFile(spill, new long[]{2, 5}, new long[0]));

        assertTrue(failure.getMessage().contains("partition 1"), failure.getMessage());
        verify(ytWriter).cancel();
        verify(ytWriter, never()).finish();
    }
}
