package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.shuffle.ShuffleReadMetricsReporter;

import tech.ytsaurus.client.AsyncReader;
import tech.ytsaurus.client.rows.UnversionedRow;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;

import scala.Function1;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

public class ShuffleRecordIterator<K, C> extends AbstractIterator<Tuple2<K, C>> {
    private final int endPartition;
    private final Function1<Object, CompletableFuture<AsyncReader<UnversionedRow>>> readerSupplier;
    private final Function1<InputStream, Iterator<Tuple2<K, C>>> deserializer;
    private final ShuffleReadMetricsReporter readMetrics;
    private int nextPartition;
    private AsyncReader<UnversionedRow> reader;
    private java.util.Iterator<UnversionedRow> rowIterator = Collections.emptyIterator();
    private UnversionedRow currentRow;
    private Iterator<Tuple2<K, C>> recordIterator = emptyIterator();

    public ShuffleRecordIterator(
            int startPartition,
            int endPartition,
            Function1<Object, CompletableFuture<AsyncReader<UnversionedRow>>> readerSupplier,
            Function1<InputStream, Iterator<Tuple2<K, C>>> deserializer,
            ShuffleReadMetricsReporter readMetrics) {
        this.nextPartition = startPartition;
        this.endPartition = endPartition;
        this.readerSupplier = readerSupplier;
        this.deserializer = deserializer;
        this.readMetrics = readMetrics;
    }

    // Scala 2.12 returns Iterator<Nothing$>, requiring the double cast and unchecked warning suppression.
    @SuppressWarnings("unchecked")
    private static <T> Iterator<T> emptyIterator() {
        return (Iterator<T>) (Iterator<?>) scala.collection.Iterator.empty();
    }

    @Override
    public boolean hasNext() {
        while (!recordIterator.hasNext()) {
            InputStream input = nextInputStream();
            if (input == null) {
                return false;
            }
            recordIterator = deserializer.apply(input);
        }
        return true;
    }

    @Override
    public Tuple2<K, C> next() {
        return recordIterator.next();
    }

    private InputStream nextInputStream() {
        Enumeration<InputStream> streams = new Enumeration<>() {
            private InputStream nextStream;
            private Long mapId;

            @Override
            public boolean hasMoreElements() {
                if (nextStream != null) {
                    return true;
                }
                UnversionedRow row = nextRow();
                if (row == null) {
                    return false;
                }
                currentRow = row;
                byte[] bytes = row.getValues().get(1).bytesValue();
                long rowMapId = ByteBuffer.wrap(bytes, 0, Long.BYTES).getLong();
                if (mapId != null && mapId != rowMapId) {
                    return false;
                }
                if (mapId == null) {
                    mapId = rowMapId;
                }
                readMetrics.incRemoteBytesRead(bytes.length);
                nextStream = new ByteArrayInputStream(bytes, Long.BYTES, bytes.length - Long.BYTES);
                currentRow = null;
                return true;
            }

            @Override
            public InputStream nextElement() {
                InputStream next = nextStream;
                nextStream = null;
                return next;
            }
        };
        return streams.hasMoreElements() ? new SequenceInputStream(streams) : null;
    }

    private UnversionedRow nextRow() {
        if (currentRow != null) {
            return currentRow;
        }
        while (!rowIterator.hasNext() && (reader != null || nextPartition < endPartition)) {
            if (reader != null) {
                var batch = reader.next().join();
                if (batch != null) {
                    rowIterator = batch.iterator();
                } else {
                    reader = null;
                }
            } else {
                reader = readerSupplier.apply(nextPartition++).join();
            }
        }
        return rowIterator.hasNext() ? rowIterator.next() : null;
    }
}
