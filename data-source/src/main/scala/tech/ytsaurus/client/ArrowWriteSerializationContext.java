package tech.ytsaurus.client;

import tech.ytsaurus.client.request.Format;
import tech.ytsaurus.client.request.WriteSerializationContext;
import tech.ytsaurus.rpcproxy.ERowsetFormat;

import java.util.HashMap;

public class ArrowWriteSerializationContext<Row> extends WriteSerializationContext<Row> {
    public ArrowWriteSerializationContext(TableRowsSerializer<Row> tableRowsSerializer) {
        super(tableRowsSerializer);
        this.rowsetFormat = ERowsetFormat.RF_FORMAT;
        this.format = new Format("arrow", new HashMap<>());
    }
}
