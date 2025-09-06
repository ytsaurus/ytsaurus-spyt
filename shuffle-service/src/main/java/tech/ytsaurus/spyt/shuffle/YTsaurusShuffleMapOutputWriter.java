package tech.ytsaurus.spyt.shuffle;

import org.apache.commons.codec.binary.Hex;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.AsyncWriter;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class YTsaurusShuffleMapOutputWriter implements ShuffleMapOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(YTsaurusShuffleMapOutputWriter.class);
    private static final List<CommitShufflePartitionsListener> commitAllPartitionListeners =
            ServiceLoader.load(CommitShufflePartitionsListener.class)
                    .stream()
                    .map(ServiceLoader.Provider::get)
                    .collect(Collectors.toList());

    private final AsyncWriter<UnversionedRow> ytsaurusWriter;
    private final long shuffleId;
    private final long mapTaskId;
    private final int rowSize;
    private final int bufferSize;
    private final long[] partitionLengths;

    private List<UnversionedRow> buffer;
    private CompletableFuture<Void> writeFuture;

    public YTsaurusShuffleMapOutputWriter(
            AsyncWriter<UnversionedRow> ytsaurusWriter,
            long shuffleId,
            long mapTaskId,
            int numPartitions,
            int rowSize,
            int bufferSize) {
        this.ytsaurusWriter = ytsaurusWriter;
        this.shuffleId = shuffleId;
        this.mapTaskId = mapTaskId;
        this.rowSize = rowSize;
        this.bufferSize = bufferSize;
        this.partitionLengths = new long[numPartitions];

        this.buffer = newBuffer();
        this.writeFuture = CompletableFuture.completedFuture(null);
    }

    @Override
    public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) {
        log.trace("GETTING PARTITION WRITER FOR {} - {} - {}", shuffleId, mapTaskId, reducePartitionId);
        return new YTsaurusShufflePartitionWriter(reducePartitionId);
    }

    @Override
    public MapOutputCommitMessage commitAllPartitions(long[] checksums) {
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
        writeFuture.join();
        ytsaurusWriter.finish().join();
        if (log.isTraceEnabled()) {
            log.trace(
                    "Bytes has been written for {} - {}; partition lengths are {}",
                    shuffleId,
                    mapTaskId,
                    Arrays.toString(partitionLengths)
            );
        }
        commitAllPartitionListeners.forEach(CommitShufflePartitionsListener::onCommitPartitions);
        return MapOutputCommitMessage.of(partitionLengths);
    }

    @Override
    public void abort(Throwable error) {
        ytsaurusWriter.cancel();
    }

    private List<UnversionedRow> newBuffer() {
        return new ArrayList<>(bufferSize);
    }

    private void addToBuffer(UnversionedRow row) {
        buffer.add(row);
        if (buffer.size() == bufferSize) {
            flushBuffer();
            buffer = newBuffer();
        }
    }

    private void flushBuffer() {
        writeFuture.join();
        writeFuture = ytsaurusWriter.write(buffer);
    }

    private class YTsaurusShufflePartitionWriter implements ShufflePartitionWriter {

        private final ByteBuffer buffer = ByteBuffer.allocate(rowSize);
        private long bytesWritten = 0L;

        private final int reducePartitionId;

        public YTsaurusShufflePartitionWriter(int reducePartitionId) {
            this.reducePartitionId = reducePartitionId;
        }

        @Override
        public OutputStream openStream() throws IOException {
            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    if (!buffer.hasRemaining()) {
                        addRow();
                    }

                    log.trace("SHUFFLE: {} MAP: {} REDUCE PARTITION: {} writing byte {}",
                            shuffleId,
                            mapTaskId,
                            reducePartitionId,
                            b
                    );
                    if (buffer.position() == 0) {
                        buffer.putLong(mapTaskId);
                    }
                    buffer.put((byte) b);
                    bytesWritten++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    if (buffer.remaining() < len) {
                        addRow();
                    }
                    if (log.isTraceEnabled()) {
                        byte[] toWrite = new byte[len];
                        System.arraycopy(b, off, toWrite, 0, len);
                        log.trace("SHUFFLE: {} MAP: {} REDUCE PARTITION: {} writing bytes {}",
                                shuffleId,
                                mapTaskId,
                                reducePartitionId,
                                Hex.encodeHexString(toWrite)
                        );
                    }
                    if (buffer.position() == 0) {
                        buffer.putLong(mapTaskId);
                    }
                    buffer.put(b, off, len);
                    bytesWritten += len;
                }

                private void addRow() {
                    buffer.flip();
                    byte[] data = new byte[buffer.limit()];
                    buffer.get(data);
                    UnversionedRow rowToWrite = new UnversionedRow(List.of(
                            new UnversionedValue(0, ColumnValueType.INT64, false, (long) reducePartitionId),
                            new UnversionedValue(1, ColumnValueType.STRING, false, data)
                    ));
                    addToBuffer(rowToWrite);
                    buffer.clear();
                }

                @Override
                public void close() {
                    if (buffer.position() > 0) {
                        addRow();
                    }
                    partitionLengths[reducePartitionId] = bytesWritten;
                }
            };
        }

        @Override
        public long getNumBytesWritten() {
            return bytesWritten;
        }
    }
}
