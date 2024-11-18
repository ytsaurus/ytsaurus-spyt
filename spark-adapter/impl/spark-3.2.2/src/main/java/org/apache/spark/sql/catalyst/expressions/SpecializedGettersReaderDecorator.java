package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.NullType;
import tech.ytsaurus.spyt.adapter.TypeSupport;
import tech.ytsaurus.spyt.patch.annotations.Applicability;
import tech.ytsaurus.spyt.patch.annotations.Decorate;
import tech.ytsaurus.spyt.patch.annotations.DecoratedMethod;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;

@Decorate
@OriginClass("org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader")
@Applicability(to = "3.3.4")
public class SpecializedGettersReaderDecorator {

    @DecoratedMethod
    public static Object read(
            SpecializedGetters obj,
            int ordinal,
            DataType dataType,
            boolean handleNull,
            boolean handleUserDefinedType) {
        if (handleNull && (obj.isNullAt(ordinal) || dataType instanceof NullType)) {
            return null;
        }
        if (TypeSupport.instance().uInt64DataType().getClass().isInstance(dataType)) {
            return obj.getLong(ordinal);
        }
        return __read(obj, ordinal, dataType, handleNull, handleUserDefinedType);
    }

    public static Object __read(
            SpecializedGetters obj,
            int ordinal,
            DataType dataType,
            boolean handleNull,
            boolean handleUserDefinedType) {
        throw new RuntimeException("Must be replaced with original method");
    }
}
