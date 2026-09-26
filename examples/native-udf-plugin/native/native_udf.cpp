#include "native_udf.h"

#include <arrow/api.h>

#include <arrow/c/bridge.h>

#include <limits>
#include <memory>
#include <string>

namespace {

/** Imports a batch and checks the homogeneous input schema required by a function. */
int32_t Import(
    ArrowArray* input,
    ArrowSchema* schema,
    int columns,
    arrow::Type::type type,
    std::shared_ptr<arrow::RecordBatch>* batch)
{
    if (!input || !schema || !input->release || !schema->release) {
        return -1;
    }
    auto result = arrow::ImportRecordBatch(input, schema);
    if (!result.ok()) {
        return -1;
    }
    *batch = std::move(result).ValueOrDie();
    if ((*batch)->num_columns() != columns) {
        return -2;
    }
    for (int i = 0; i < columns; ++i) {
        if ((*batch)->column(i)->type_id() != type) {
            return -2;
        }
    }
    return 0;
}

/** Finishes a builder and transfers a one-column record batch to the C Data consumer. */
template <typename Builder>
int32_t Export(Builder* builder, ArrowArray* output, ArrowSchema* outputSchema)
{
    if (!output || !outputSchema || output->release || outputSchema->release) {
        return -3;
    }
    std::shared_ptr<arrow::Array> array;
    if (!builder->Finish(&array).ok()) {
        return -3;
    }
    auto schema = arrow::schema({arrow::field("value", array->type(), true)});
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});
    return arrow::ExportRecordBatch(*batch, output, outputSchema).ok() ? 0 : -3;
}

} // namespace

int32_t IncrementInt64(
    ArrowArray* input,
    ArrowSchema* schema,
    int64_t delta,
    ArrowArray* output,
    ArrowSchema* outputSchema)
{
    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = Import(input, schema, 1, arrow::Type::INT64, &batch);
        if (status != 0) {
            return status;
        }
        const auto column = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        arrow::Int64Builder builder;
        if (!builder.Reserve(batch->num_rows()).ok()) {
            return -3;
        }
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            if (column->IsNull(row)) {
                if (!builder.AppendNull().ok()) {
                    return -3;
                }
                continue;
            }
            const auto value = column->Value(row);
            if ((delta > 0 && value > std::numeric_limits<int64_t>::max() - delta) ||
                (delta < 0 && value < std::numeric_limits<int64_t>::min() - delta))
            {
                return -4;
            }
            if (!builder.Append(value + delta).ok()) {
                return -3;
            }
        }
        return Export(&builder, output, outputSchema);
    } catch (...) {
        return -3;
    }
}

int32_t ScaleInt64ToDouble(
    ArrowArray* input,
    ArrowSchema* schema,
    double factor,
    ArrowArray* output,
    ArrowSchema* outputSchema)
{
    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = Import(input, schema, 1, arrow::Type::INT64, &batch);
        if (status != 0) {
            return status;
        }
        const auto column = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        arrow::DoubleBuilder builder;
        if (!builder.Reserve(batch->num_rows()).ok()) {
            return -3;
        }
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            const auto appended = column->IsNull(row) ? builder.AppendNull()
                : builder.Append(static_cast<double>(column->Value(row)) * factor);
            if (!appended.ok()) {
                return -3;
            }
        }
        return Export(&builder, output, outputSchema);
    } catch (...) {
        return -3;
    }
}

int32_t Utf8Lengths(
    ArrowArray* input,
    ArrowSchema* schema,
    ArrowArray* output,
    ArrowSchema* outputSchema)
{
    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = Import(input, schema, 1, arrow::Type::STRING, &batch);
        if (status != 0) {
            return status;
        }
        const auto column = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        arrow::Int32Builder builder;
        if (!builder.Reserve(batch->num_rows()).ok()) {
            return -3;
        }
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            const auto appended = column->IsNull(row) ? builder.AppendNull()
                : builder.Append(column->value_length(row));
            if (!appended.ok()) {
                return -3;
            }
        }
        return Export(&builder, output, outputSchema);
    } catch (...) {
        return -3;
    }
}

int32_t ConcatUtf8(
    ArrowArray* input,
    ArrowSchema* schema,
    ArrowArray* output,
    ArrowSchema* outputSchema)
{
    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = Import(input, schema, 2, arrow::Type::STRING, &batch);
        if (status != 0) {
            return status;
        }
        const auto left = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        const auto right = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        arrow::StringBuilder builder;
        if (!builder.Reserve(batch->num_rows()).ok()) {
            return -3;
        }
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            if (left->IsNull(row) || right->IsNull(row)) {
                if (!builder.AppendNull().ok()) {
                    return -3;
                }
            } else {
                const auto length = static_cast<int64_t>(left->value_length(row)) + right->value_length(row);
                if (length > std::numeric_limits<int32_t>::max()) {
                    return -4;
                }
                const std::string value = left->GetString(row) + right->GetString(row);
                if (!builder.Append(value).ok()) {
                    return -3;
                }
            }
        }
        return Export(&builder, output, outputSchema);
    } catch (...) {
        return -3;
    }
}
