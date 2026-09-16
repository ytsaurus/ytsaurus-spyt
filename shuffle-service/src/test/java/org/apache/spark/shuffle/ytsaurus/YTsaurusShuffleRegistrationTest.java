package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.HashPartitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.CompoundClient;
import tech.ytsaurus.client.request.ShuffleHandle;
import tech.ytsaurus.client.request.StartShuffle;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.GUID;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.spark.shuffle.ytsaurus.Config.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class YTsaurusShuffleRegistrationTest {
    private final SparkConf conf = new SparkConf(false);
    private final CompoundClient client = mock(CompoundClient.class);
    private final SortShuffleManager delegate = mock(SortShuffleManager.class);
    private final ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
    @SuppressWarnings("unchecked")
    private final ShuffleDependency<Integer, String, String> dependency = mock(ShuffleDependency.class);
    private final GUID transactionId = GUID.create();
    private BaseShuffleHandle<Integer, String, String> baseHandle;
    private YTsaurusShuffleManager manager;

    @BeforeEach
    void setUp() {
        when(dependency.partitioner()).thenReturn(new HashPartitioner(2));
        baseHandle = new BaseShuffleHandle<>(1, dependency);
        when(delegate.registerShuffle(1, dependency)).thenReturn(baseHandle);
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
        when(transaction.getId()).thenReturn(transactionId);
        when(transaction.abort()).thenReturn(CompletableFuture.completedFuture(null));
        manager = new YTsaurusShuffleManager(conf, client, delegate);
    }

    @Test
    void validatesConfigurationBeforeRegistration() {
        conf.set(YTSAURUS_SHUFFLE_CONFIG.key(), "{unexpected={}}");

        assertThrows(IllegalArgumentException.class, () -> manager.registerShuffle(1, dependency));

        verifyNoInteractions(client, delegate, transaction);
        assertFalse(conf.contains(shuffleTransactionId(1)));
    }

    @Test
    void unregistersDelegateWhenTransactionCreationFails() {
        RuntimeException failure = new IllegalStateException("transaction failed");
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(CompletableFuture.failedFuture(failure));

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> manager.registerShuffle(1, dependency));

        assertSame(failure, thrown.getCause());
        verify(delegate).unregisterShuffle(1);
        verify(client, never()).startShuffle(any(StartShuffle.class));
        verify(transaction, never()).abort();
        assertFalse(conf.contains(shuffleTransactionId(1)));
    }

    @Test
    void abortsTransactionAndUnregistersDelegateWhenShuffleStartFails() {
        RuntimeException failure = new IllegalStateException("shuffle failed");
        when(client.startShuffle(any(StartShuffle.class))).thenReturn(CompletableFuture.failedFuture(failure));

        CompletionException thrown = assertThrows(CompletionException.class,
                () -> manager.registerShuffle(1, dependency));

        assertSame(failure, thrown.getCause());
        verify(transaction).abort();
        verify(delegate).unregisterShuffle(1);
        assertFalse(conf.contains(shuffleTransactionId(1)));
    }

    @Test
    void preservesOriginalFailureWhenCleanupFails() {
        RuntimeException failure = new IllegalStateException("shuffle failed");
        RuntimeException abortFailure = new IllegalStateException("abort failed");
        RuntimeException unregisterFailure = new IllegalStateException("unregister failed");
        when(client.startShuffle(any(StartShuffle.class))).thenThrow(failure);
        when(transaction.abort()).thenReturn(CompletableFuture.failedFuture(abortFailure));
        when(delegate.unregisterShuffle(1)).thenThrow(unregisterFailure);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> manager.registerShuffle(1, dependency)));

        verify(transaction).abort();
        verify(delegate).unregisterShuffle(1);
        assertEquals(2, failure.getSuppressed().length);
        assertSame(abortFailure, failure.getSuppressed()[0].getCause());
        assertSame(unregisterFailure, failure.getSuppressed()[1]);
        assertFalse(conf.contains(shuffleTransactionId(1)));
    }

    @Test
    void retainsTransactionAndDelegateAfterSuccessfulRegistration() {
        ShuffleHandle ytHandle = mock(ShuffleHandle.class);
        when(client.startShuffle(any(StartShuffle.class))).thenReturn(CompletableFuture.completedFuture(ytHandle));

        CompoundShuffleHandle<?, ?, ?> handle = (CompoundShuffleHandle<?, ?, ?>) manager.registerShuffle(1, dependency);

        assertSame(baseHandle, handle.baseHandle());
        assertSame(ytHandle, handle.ytHandle());
        assertEquals(2, handle.partitionCount());
        assertEquals(transactionId.toString(), conf.get(shuffleTransactionId(1)));
        verify(transaction, never()).abort();
        verify(delegate, never()).unregisterShuffle(1);
    }
}
