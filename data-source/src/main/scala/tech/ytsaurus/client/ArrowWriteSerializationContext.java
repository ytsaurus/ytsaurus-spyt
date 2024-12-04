package tech.ytsaurus.client;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.rpcproxy.ERowsetFormat;

import java.util.HashMap;
import java.util.Map;

public class ArrowWriteSerializationContext<Row> extends SerializationContext<Row> {
    private final java.util.List<? extends Map.Entry<String, ? extends YTGetters.FromStruct<Row>>> rowGetters;

    public ArrowWriteSerializationContext(
            java.util.List<? extends Map.Entry<String, ? extends YTGetters.FromStruct<Row>>> rowGetters
    ) {
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.format = new Format("arrow", new HashMap<>());
        this.rowGetters = rowGetters;
    }

    public java.util.List<? extends Map.Entry<String, ? extends YTGetters.FromStruct<Row>>> getRowGetters() {
        return rowGetters;
    }
}
