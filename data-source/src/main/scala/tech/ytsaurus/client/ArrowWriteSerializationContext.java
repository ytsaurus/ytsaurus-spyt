package tech.ytsaurus.client;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.rpcproxy.ERowsetFormat;

import java.util.HashMap;
import java.util.Map;

public class ArrowWriteSerializationContext<T, L, D, G extends YTGetters<T, L, D>> extends SerializationContext<T> {
    private final java.util.List<? extends Map.Entry<String, ? extends G.FromStruct>> rowGetters;

    public ArrowWriteSerializationContext(
            java.util.List<? extends Map.Entry<String, ? extends G.FromStruct>> rowGetters
    ) {
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.format = new Format("arrow", new HashMap<>());
        this.rowGetters = rowGetters;
    }

    public java.util.List<? extends Map.Entry<String, ? extends G.FromStruct>> getRowGetters() {
        return rowGetters;
    }
}
