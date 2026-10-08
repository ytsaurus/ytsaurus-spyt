package tech.ytsaurus.spyt.shuffle;

import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.TimeTrackingOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Transfers the single spill file of a map task to YTsaurus shuffle partition by partition.
 * Spark prefers this writer to its standard merge path when a map task produced exactly one spill file,
 * and it does not count the transferred bytes again: they were already reported as shuffle bytes written
 * while the spill file was produced. The standard merge path counts them a second time.
 */
public class YTsaurusSingleSpillMapOutputWriter implements SingleSpillShuffleMapOutputWriter {
    // Upper bound of one copy, so that large rows do not need a large copy buffer.
    private static final int MAX_COPY_CHUNK_SIZE = 64 * 1024;

    private final IntFunction<ShuffleMapOutputWriter> mapOutputWriterFactory;
    private final int copyChunkSize;
    private final ShuffleWriteMetricsReporter writeMetrics;

    /**
     * @param mapOutputWriterFactory creates the map output writer for the given number of reduce partitions;
     *                               the number is known only when the spill file is transferred
     * @param rowSize the value of spark.ytsaurus.shuffle.write.row.size; a partition writer takes each write into one
     *                row together with the 8 byte map id header, so a copy chunk leaves room for that header
     * @param writeMetrics receives the time spent transferring the file; the shuffle manager sets a reporter
     *                     for every map task before the writer is created
     */
    public YTsaurusSingleSpillMapOutputWriter(
            IntFunction<ShuffleMapOutputWriter> mapOutputWriterFactory,
            int rowSize,
            ShuffleWriteMetricsReporter writeMetrics) {
        this.mapOutputWriterFactory = mapOutputWriterFactory;
        this.copyChunkSize = Math.min(MAX_COPY_CHUNK_SIZE, rowSize - Long.BYTES);
        this.writeMetrics = Objects.requireNonNull(writeMetrics, "shuffle write metrics reporter is not set");
    }

    @Override
    public void transferMapSpillFile(File mapSpillFile, long[] partitionLengths, long[] checksums)
            throws IOException {
        final ShuffleMapOutputWriter mapOutputWriter = mapOutputWriterFactory.apply(partitionLengths.length);
        try (InputStream spillInput = new BufferedInputStream(new FileInputStream(mapSpillFile), copyChunkSize)) {
            final byte[] copyBuffer = new byte[copyChunkSize];
            for (int partition = 0; partition < partitionLengths.length; partition++) {
                final ShufflePartitionWriter partitionWriter = mapOutputWriter.getPartitionWriter(partition);
                try (OutputStream partitionOutput = new TimeTrackingOutputStream(writeMetrics,
                        partitionWriter.openStream())) {
                    copyPartition(spillInput, partitionOutput, partitionLengths[partition], copyBuffer, mapSpillFile,
                            partition);
                }
            }
            mapOutputWriter.commitAllPartitions(checksums);
        } catch (Throwable failure) {
            abort(mapOutputWriter, failure);
            throw failure;
        }
    }

    private static void copyPartition(
            InputStream spillInput,
            OutputStream partitionOutput,
            long partitionLength,
            byte[] copyBuffer,
            File mapSpillFile,
            int partition) throws IOException {
        long remaining = partitionLength;
        while (remaining > 0) {
            final int read = spillInput.read(copyBuffer, 0, (int) Math.min(copyBuffer.length, remaining));
            if (read < 0) {
                throw new IOException("Spill file " + mapSpillFile + " ended " + remaining + " bytes before the end of "
                        + "partition " + partition + ", which is expected to hold " + partitionLength + " bytes");
            }
            partitionOutput.write(copyBuffer, 0, read);
            remaining -= read;
        }
    }

    private static void abort(ShuffleMapOutputWriter mapOutputWriter, Throwable failure) {
        try {
            mapOutputWriter.abort(failure);
        } catch (IOException | RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }
}
