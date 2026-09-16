package org.apache.spark.shuffle.ytsaurus;

import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleHandle;

public class CompoundShuffleHandle<K, V, C> extends ShuffleHandle {
    private static final long serialVersionUID = 4612845889574484741L;

    private final BaseShuffleHandle<K, V, C> baseHandle;
    private final tech.ytsaurus.client.request.ShuffleHandle ytHandle;
    private final int partitionCount;

    public CompoundShuffleHandle(
            BaseShuffleHandle<K, V, C> baseHandle,
            tech.ytsaurus.client.request.ShuffleHandle ytHandle,
            int partitionCount) {
        super(baseHandle.shuffleId());
        this.baseHandle = baseHandle;
        this.ytHandle = ytHandle;
        this.partitionCount = partitionCount;
    }

    public BaseShuffleHandle<K, V, C> baseHandle() {
        return baseHandle;
    }

    public tech.ytsaurus.client.request.ShuffleHandle ytHandle() {
        return ytHandle;
    }

    public int partitionCount() {
        return partitionCount;
    }
}
