#pragma once

#include <arrow/c/abi.h>
#include <util/system/compiler.h>

#include <cstdint>

/**
 * Example native ABI consumed by the companion Java providers.
 *
 * Every input is an exported Arrow record batch. Import consumes its C Data
 * ownership references, including schema callbacks; the original Java vectors
 * remain owned by Java. Calls return 0 on success, -1 on import failure, -2 on
 * incompatible input types or arity, -3 on allocation/export/native failure,
 * and -4 on integer overflow. No C++ exception crosses this boundary.
 *
 * Output structs must be zero-initialized. Successful output exports own their
 * buffers through release callbacks, which the Java importer must eventually
 * invoke. On failure the caller must release any populated output structs.
 */
extern "C" {

/**
 * Adds delta to one nullable INT64 column, returning a new INT64 field named value.
 * Input buffers remain unchanged, including when an overflow is reported.
 */
Y_PUBLIC int32_t IncrementInt64(ArrowArray* input, ArrowSchema* schema, int64_t delta,
                              ArrowArray* output, ArrowSchema* outputSchema);

/** Converts one nullable INT64 column to a nullable FLOAT64 field named value. */
Y_PUBLIC int32_t ScaleInt64ToDouble(ArrowArray* input, ArrowSchema* schema, double factor,
                                  ArrowArray* output, ArrowSchema* outputSchema);

/** Returns UTF-8 byte lengths of one nullable UTF8 column as nullable INT32 value. */
Y_PUBLIC int32_t Utf8Lengths(ArrowArray* input, ArrowSchema* schema,
                           ArrowArray* output, ArrowSchema* outputSchema);

/** Concatenates two UTF8 columns into UTF8 value; either null input produces null. */
Y_PUBLIC int32_t ConcatUtf8(ArrowArray* input, ArrowSchema* schema,
                          ArrowArray* output, ArrowSchema* outputSchema);

}
