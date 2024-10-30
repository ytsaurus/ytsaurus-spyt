package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.rpcproxy.TWriteTableMeta;


@NonNullApi
class TableWriterBaseImpl<T> extends RawTableWriterImpl {
    protected @Nullable
    TableSchema schema;
    protected final WriteTable<T> req;
    protected @Nullable
    TableRowsSerializer<T> tableRowsSerializer;
    private final SerializationResolver serializationResolver;
    @Nullable
    protected ApiServiceTransaction transaction;

    TableWriterBaseImpl(WriteTable<T> req, SerializationResolver serializationResolver) {
        super(req.getWindowSize(), req.getPacketSize());
        this.req = req;
        this.serializationResolver = serializationResolver;
        var format = this.req.getSerializationContext().getFormat();
        if (format.isEmpty() || !"arrow".equals(format.get().getType())) {
            tableRowsSerializer = TableRowsSerializer.createTableRowsSerializer(
                    this.req.getSerializationContext(), serializationResolver
            ).orElse(null);
        }
    }

    public void setTransaction(ApiServiceTransaction transaction) {
        if (this.transaction != null) {
            throw new IllegalStateException("Write transaction already started");
        }
        this.transaction = transaction;
    }

    public CompletableFuture<TableWriterBaseImpl<T>> startUploadImpl() {
        TableWriterBaseImpl<T> self = this;

        return startUpload.thenApply((attachments) -> {
            if (attachments.size() != 1) {
                throw new IllegalArgumentException("protocol error");
            }
            byte[] head = attachments.get(0);
            if (head == null) {
                throw new IllegalArgumentException("protocol error");
            }

            TWriteTableMeta metadata = RpcUtil.parseMessageBodyWithCompression(
                    head,
                    TWriteTableMeta.parser(),
                    Compression.None
            );
            self.schema = ApiServiceUtil.deserializeTableSchema(metadata.getSchema());
            logger.debug("schema -> {}", schema.toYTree().toString());

            {
                var format = this.req.getSerializationContext().getFormat();
                if (format.isPresent() && "arrow".equals(format.get().getType())) {
                    tableRowsSerializer = new ArrowTableRowsSerializer<>(
                            ((ArrowWriteSerializationContext<T, ?, ?, ?>) this.req.getSerializationContext()).getRowGetters()
                    );
                }
            }

            if (this.tableRowsSerializer == null) {
                if (this.req.getSerializationContext().getObjectClass().isEmpty()) {
                    throw new IllegalStateException("No object clazz");
                }
                Class<T> objectClazz = self.req.getSerializationContext().getObjectClass().get();
                if (UnversionedRow.class.equals(objectClazz)) {
                    this.tableRowsSerializer =
                            (TableRowsSerializer<T>) new TableRowsWireSerializer<>(new UnversionedRowSerializer());
                } else {
                    this.tableRowsSerializer = new TableRowsWireSerializer<>(
                            serializationResolver.createWireRowSerializer(
                                    serializationResolver.forClass(objectClazz, self.schema))
                    );
                }
            }

            return self;
        });
    }

    public boolean write(List<T> rows, TableSchema schema) throws IOException {
        byte[] serializedRows = tableRowsSerializer.serializeRows(rows, schema);
        return write(serializedRows);
    }

    @Override
    public CompletableFuture<?> close() {
        return super.close()
                .thenCompose(response -> {
                    if (transaction != null && transaction.isActive()) {
                        return transaction.commit()
                                .thenApply(unused -> response);
                    }
                    return CompletableFuture.completedFuture(response);
                });
    }
}
