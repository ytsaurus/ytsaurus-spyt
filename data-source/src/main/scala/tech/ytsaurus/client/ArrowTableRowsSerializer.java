package tech.ytsaurus.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.spyt.format.batch.ArrowUtils;
import tech.ytsaurus.typeinfo.DecimalType;
import tech.ytsaurus.yson.YsonBinaryWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrowTableRowsSerializer<Row> extends TableRowsSerializerBase<Row> {
    private static abstract class ArrowGetterFromStruct<Row> {
        public final Field field;
        public final ArrowType arrowType;

        ArrowGetterFromStruct(Field field) {
            super();
            this.field = field;
            this.arrowType = field.getType();
        }

        public abstract ArrowWriterFromStruct<Row> writer(ValueVector valueVector);
    }

    private interface ArrowWriterFromStruct<Row> {
        void setFromStruct(Row struct);
    }

    private static abstract class ArrowGetterFromList<List> {
        public final Field field;
        public final ArrowType arrowType;

        ArrowGetterFromList(Field field) {
            this.field = field;
            this.arrowType = field.getType();
        }

        public abstract ArrowWriterFromList<List> writer(ValueVector valueVector);
    }

    private interface ArrowWriterFromList<List> {
        void setFromList(List list, int i);
    }

    private <Array> ArrowGetterFromList<Array> arrowGetter(String name, YTGetters.FromList<Array> getter) {
        var optionalGetter = getter instanceof YTGetters.FromListToOptional
                ? (YTGetters.FromListToOptional<Array>) getter
                : null;
        var nonEmptyGetter = optionalGetter != null ? optionalGetter.getNotEmptyGetter() : getter;
        var arrowGetter = Objects.requireNonNullElseGet(nonComplexArrowGetter(name, nonEmptyGetter), () ->
                new ArrowGetterFromList<Array>(new Field(name, new FieldType(
                        false, new ArrowType.Binary(), null
                ), new ArrayList<>())) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return (array, i) -> {
                            var byteArrayOutputStream = new ByteArrayOutputStream();
                            try (var ysonBinaryWriter = new YsonBinaryWriter(byteArrayOutputStream)) {
                                nonEmptyGetter.getYson(array, i, ysonBinaryWriter);
                            }
                            varBinaryVector.setSafe(varBinaryVector.getValueCount(), byteArrayOutputStream.toByteArray());
                        };
                    }
                }
        );
        return new ArrowGetterFromList<>(new Field(name, new FieldType(
                optionalGetter != null, arrowGetter.field.getType(), null
        ), arrowGetter.field.getChildren())) {
            @Override
            public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                var nonOptionalWriter = arrowGetter.writer(valueVector);
                var fieldVector = optionalGetter == null ? null : (FieldVector) valueVector;
                return (array, i) -> {
                    if (optionalGetter != null && optionalGetter.isEmpty(array, i)) {
                        fieldVector.setNull(valueVector.getValueCount());
                    } else {
                        nonOptionalWriter.setFromList(array, i);
                    }
                    valueVector.setValueCount(valueVector.getValueCount() + 1);
                };
            }
        };
    }

    private <Struct> ArrowGetterFromStruct<Struct> arrowGetter(String name, YTGetters.FromStruct<Struct> getter) {
        var optionalGetter = getter instanceof YTGetters.FromStructToOptional
                ? (YTGetters.FromStructToOptional<Struct>) getter
                : null;
        var nonEmptyGetter = optionalGetter != null ? optionalGetter.getNotEmptyGetter() : getter;
        var arrowGetter = Objects.requireNonNullElseGet(nonComplexArrowGetter(name, nonEmptyGetter), () ->
                new ArrowGetterFromStruct<Struct>(new Field(name, new FieldType(
                        false, new ArrowType.Binary(), null
                ), new ArrayList<>())) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return struct -> {
                            var byteArrayOutputStream = new ByteArrayOutputStream();
                            try (var ysonBinaryWriter = new YsonBinaryWriter(byteArrayOutputStream)) {
                                nonEmptyGetter.getYson(struct, ysonBinaryWriter);
                            }
                            varBinaryVector.setSafe(varBinaryVector.getValueCount(), byteArrayOutputStream.toByteArray());
                        };
                    }
                }
        );
        return new ArrowGetterFromStruct<>(new Field(name, new FieldType(
                optionalGetter != null, arrowGetter.field.getType(), null
        ), arrowGetter.field.getChildren())) {
            @Override
            public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                var nonOptionalWriter = arrowGetter.writer(valueVector);
                var fieldVector = optionalGetter == null ? null : (FieldVector) valueVector;
                return struct -> {
                    if (optionalGetter != null && optionalGetter.isEmpty(struct)) {
                        fieldVector.setNull(valueVector.getValueCount());
                    } else {
                        nonOptionalWriter.setFromStruct(struct);
                    }
                    valueVector.setValueCount(valueVector.getValueCount() + 1);
                };
            }
        };
    }

    private Field field(String name, ArrowType arrowType) {
        return new Field(name, new FieldType(false, arrowType, null), Collections.emptyList());
    }

    private <Array> ArrowGetterFromList<Array> nonComplexArrowGetter(String name, YTGetters.FromList<Array> getter) {
        var tiType = getter.getTiType();
        switch (tiType.getTypeName()) {
            case Null:
            case Void: {
                return new ArrowGetterFromList<>(
                        new Field(name, new FieldType(false, new ArrowType.Null(), null), new ArrayList<>())
                ) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        return (list, i) -> {};
                    }
                };
            }
            case Utf8:
            case String: {
                var stringGetter = (YTGetters.FromListToString<Array>) getter;
                return new ArrowGetterFromList<>(
                        new Field(name, new FieldType(false, new ArrowType.Binary(), null), new ArrayList<>())
                ) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return (list, i) -> {
                            var byteBuffer = stringGetter.getString(list, i);
                            varBinaryVector.setSafe(
                                    varBinaryVector.getValueCount(),
                                    byteBuffer, byteBuffer.position(), byteBuffer.remaining()
                            );
                        };
                    }
                };
            }
            case Int8: {
                var byteGetter = (YTGetters.FromListToByte<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(8, true))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var tinyIntVector = (TinyIntVector) valueVector;
                        return (list, i) ->
                                tinyIntVector.set(tinyIntVector.getValueCount(), byteGetter.getByte(list, i));
                    }
                };
            }
            case Uint8: {
                var byteGetter = (YTGetters.FromListToByte<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(8, false))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var uInt1Vector = (UInt1Vector) valueVector;
                        return (list, i) ->
                                uInt1Vector.set(uInt1Vector.getValueCount(), byteGetter.getByte(list, i));
                    }
                };
            }
            case Int16: {
                var shortGetter = (YTGetters.FromListToShort<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(16, true))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var smallIntVector = (SmallIntVector) valueVector;
                        return (list, i) ->
                                smallIntVector.set(smallIntVector.getValueCount(), shortGetter.getShort(list, i));
                    }
                };
            }
            case Uint16: {
                var shortGetter = (YTGetters.FromListToShort<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(16, false))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var uInt2Vector = (UInt2Vector) valueVector;
                        return (list, i) ->
                                uInt2Vector.set(uInt2Vector.getValueCount(), shortGetter.getShort(list, i));
                    }
                };
            }
            case Int32: {
                var intGetter = (YTGetters.FromListToInt<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(32, true))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var intVector = (IntVector) valueVector;
                        return (list, i) ->
                                intVector.set(intVector.getValueCount(), intGetter.getInt(list, i));
                    }
                };
            }
            case Uint32: {
                var intGetter = (YTGetters.FromListToInt<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(32, false))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var uInt4Vector = (UInt4Vector) valueVector;
                        return (list, i) ->
                                uInt4Vector.set(uInt4Vector.getValueCount(), intGetter.getInt(list, i));
                    }
                };
            }
            case Interval:
            case Interval64:
            case Int64: {
                var longGetter = (YTGetters.FromListToLong<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(64, true))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var bigIntVector = (BigIntVector) valueVector;
                        return (list, i) ->
                                bigIntVector.set(bigIntVector.getValueCount(), longGetter.getLong(list, i));
                    }
                };
            }
            case Uint64: {
                var longGetter = (YTGetters.FromListToLong<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Int(64, false))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var uInt8Vector = (UInt8Vector) valueVector;
                        return (list, i) ->
                                uInt8Vector.set(uInt8Vector.getValueCount(), longGetter.getLong(list, i));
                    }
                };
            }
            case Bool: {
                var booleanGetter = (YTGetters.FromListToBoolean<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Bool())) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var bitVector = (BitVector) valueVector;
                        return (list, i) ->
                                bitVector.set(bitVector.getValueCount(), booleanGetter.getBoolean(list, i) ? 1 : 0);
                    }
                };
            }
            case Float: {
                var floatGetter = (YTGetters.FromListToFloat<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var float4Vector = (Float4Vector) valueVector;
                        return (list, i) ->
                                float4Vector.set(float4Vector.getValueCount(), floatGetter.getFloat(list, i));
                    }
                };
            }
            case Double: {
                var doubleGetter = (YTGetters.FromListToDouble<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var float8Vector = (Float8Vector) valueVector;
                        return (list, i) ->
                                float8Vector.set(float8Vector.getValueCount(), doubleGetter.getDouble(list, i));
                    }
                };
            }
            case Decimal: {
                var decimalGetter = (YTGetters.FromListToBigDecimal<Array>) getter;
                var decimalType = (DecimalType) decimalGetter.getTiType();
                return new ArrowGetterFromList<>(field(name, new ArrowType.Decimal(
                        decimalType.getPrecision(), decimalType.getScale(), 128
                ))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var decimalVector = (DecimalVector) valueVector;
                        return (list, i) ->
                                decimalVector.set(decimalVector.getValueCount(), decimalGetter.getBigDecimal(list, i));
                    }
                };
            }
            case Date:
            case Date32: {
                var intGetter = (YTGetters.FromListToInt<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Date(DateUnit.DAY))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var dateDayVector = (DateDayVector) valueVector;
                        return (list, i) ->
                                dateDayVector.set(dateDayVector.getValueCount(), intGetter.getInt(list, i));
                    }
                };
            }
            case Datetime:
            case Datetime64: {
                var longGetter = (YTGetters.FromListToLong<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Date(DateUnit.MILLISECOND))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var dateMilliVector = (DateMilliVector) valueVector;
                        return (list, i) ->
                                dateMilliVector.set(dateMilliVector.getValueCount(), longGetter.getLong(list, i));
                    }
                };
            }
            case Timestamp:
            case Timestamp64: {
                var longGetter = (YTGetters.FromListToLong<Array>) getter;
                return new ArrowGetterFromList<>(field(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))) {
                    @Override
                    public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                        var timeStampMicroVector = (TimeStampMicroVector) valueVector;
                        return (list, i) ->
                                timeStampMicroVector.set(timeStampMicroVector.getValueCount(), longGetter.getLong(list, i));
                    }
                };
            }
            case List: {
                return getArrowGetterFromList(name, (YTGetters.FromListToList<Array, ?>) getter);
            }
            case Dict: {
                return getArrowGetterFromList(name, (YTGetters.FromListToDict<Array, ?, ?, ?>) getter);
            }
            case Struct: {
                return getArrowGetterFromList(name, (YTGetters.FromListToStruct<Array, ?>) getter);
            }
            default:
                return null;
        }
    }

    private <Array, Struct> ArrowGetterFromList<Array> getArrowGetterFromList(
            String name, YTGetters.FromListToStruct<Array, Struct> structGetter
    ) {
        var members = structGetter.getMembersGetters();
        var membersGetters = new ArrayList<ArrowGetterFromStruct<Struct>>(members.size());
        for (var member : members) {
            membersGetters.add(arrowGetter(member.getKey(), member.getValue()));
        }
        return new ArrowGetterFromList<>(new Field(
                name, new FieldType(false, new ArrowType.Struct(), null),
                membersGetters.stream().map(member -> member.field).collect(Collectors.toList())
        )) {
            @Override
            public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                var structVector = (StructVector) valueVector;
                var membersWriters = new ArrayList<ArrowWriterFromStruct<Struct>>(members.size());
                for (int i = 0; i < members.size(); i++) {
                    membersWriters.add(membersGetters.get(i).writer(structVector.getChildByOrdinal(i)));
                }
                return (list, i) -> {
                    var struct = structGetter.getStruct(list, i);
                    structVector.setIndexDefined(structVector.getValueCount());
                    for (int j = 0; j < members.size(); j++) {
                        membersWriters.get(j).setFromStruct(struct);
                    }
                };
            }
        };
    }

    private <Array, Dict, Keys, Values> ArrowGetterFromList<Array> getArrowGetterFromList(
            String name, YTGetters.FromListToDict<Array, Dict, Keys, Values> dictGetter
    ) {
        var fromDictGetter = dictGetter.getGetter();
        var keyGetter = nonComplexArrowGetter("key", fromDictGetter.getKeyGetter());
        var valueGetter = arrowGetter("value", fromDictGetter.getValueGetter());
        if (keyGetter == null) {
            return null;
        }
        return new ArrowGetterFromList<>(new Field(
                name, new FieldType(false, new ArrowType.Map(false), null),
                Collections.singletonList(new Field(
                        "entries", new FieldType(false, new ArrowType.Struct(), null),
                        Arrays.asList(keyGetter.field, valueGetter.field)
                ))
        )) {
            @Override
            public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                var mapVector = (MapVector) valueVector;
                var structVector = (StructVector) mapVector.getDataVector();
                var keyWriter = keyGetter.writer(structVector.getChildByOrdinal(0));
                var valueWriter = valueGetter.writer(structVector.getChildByOrdinal(1));
                return (list, i) -> {
                    var dict = dictGetter.getDict(list, i);
                    int size = fromDictGetter.getSize(dict);
                    var keys = fromDictGetter.getKeys(dict);
                    var values = fromDictGetter.getValues(dict);
                    mapVector.startNewValue(mapVector.getValueCount());
                    for (int j = 0; j < size; j++) {
                        structVector.setIndexDefined(structVector.getValueCount());
                        keyWriter.setFromList(keys, j);
                        valueWriter.setFromList(values, j);
                        structVector.setValueCount(structVector.getValueCount() + 1);
                    }
                    mapVector.endValue(mapVector.getValueCount(), size);
                };
            }
        };
    }

    private <Array, Value> ArrowGetterFromList<Array> getArrowGetterFromList(
            String name, YTGetters.FromListToList<Array, Value> listGetter
    ) {
        var elementGetter = listGetter.getElementGetter();
        var itemGetter = arrowGetter("item", elementGetter);
        return new ArrowGetterFromList<>(new Field(name, new FieldType(
                false, new ArrowType.List(), null
        ), Collections.singletonList(itemGetter.field))) {
            @Override
            public ArrowWriterFromList<Array> writer(ValueVector valueVector) {
                var listVector = (ListVector) valueVector;
                var dataWriter = itemGetter.writer(listVector.getDataVector());
                return (list, i) -> {
                    var value = listGetter.getList(list, i);
                    int size = elementGetter.getSize(value);
                    listVector.startNewValue(listVector.getValueCount());
                    for (int j = 0; j < size; j++) {
                        dataWriter.setFromList(value, j);
                    }
                    listVector.endValue(listVector.getValueCount(), size);
                };
            }
        };
    }

    private <Struct> ArrowGetterFromStruct<Struct> nonComplexArrowGetter(String name, YTGetters.FromStruct<Struct> getter) {
        var tiType = getter.getTiType();
        switch (tiType.getTypeName()) {
            case Null:
            case Void: {
                return new ArrowGetterFromStruct<>(
                        new Field(name, new FieldType(false, new ArrowType.Null(), null), new ArrayList<>())
                ) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        return struct -> {};
                    }
                };
            }
            case Utf8:
            case String: {
                var stringGetter = (YTGetters.FromStructToString<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Binary())) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return struct -> {
                            var byteBuffer = stringGetter.getString(struct);
                            varBinaryVector.setSafe(
                                    varBinaryVector.getValueCount(),
                                    byteBuffer, byteBuffer.position(), byteBuffer.remaining()
                            );
                        };
                    }
                };
            }
            case Int8: {
                var byteGetter = (YTGetters.FromStructToByte<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(8, true))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var tinyIntVector = (TinyIntVector) valueVector;
                        return struct -> tinyIntVector.set(tinyIntVector.getValueCount(), byteGetter.getByte(struct));
                    }
                };
            }
            case Uint8: {
                var byteGetter = (YTGetters.FromStructToByte<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(8, false))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var uInt1Vector = (UInt1Vector) valueVector;
                        return struct ->
                                uInt1Vector.set(uInt1Vector.getValueCount(), byteGetter.getByte(struct));
                    }
                };
            }
            case Int16: {
                var shortGetter = (YTGetters.FromStructToShort<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(16, true))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var smallIntVector = (SmallIntVector) valueVector;
                        return struct ->
                                smallIntVector.set(smallIntVector.getValueCount(), shortGetter.getShort(struct));
                    }
                };
            }
            case Uint16: {
                var shortGetter = (YTGetters.FromStructToShort<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(16, false))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var uInt2Vector = (UInt2Vector) valueVector;
                        return struct -> uInt2Vector.set(uInt2Vector.getValueCount(), shortGetter.getShort(struct));
                    }
                };
            }
            case Int32: {
                var intGetter = (YTGetters.FromStructToInt<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(32, true))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var intVector = (IntVector) valueVector;
                        return struct -> intVector.set(intVector.getValueCount(), intGetter.getInt(struct));
                    }
                };
            }
            case Uint32: {
                var intGetter = (YTGetters.FromStructToInt<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(32, false))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var uInt4Vector = (UInt4Vector) valueVector;
                        return struct -> uInt4Vector.set(uInt4Vector.getValueCount(), intGetter.getInt(struct));
                    }
                };
            }
            case Interval:
            case Interval64:
            case Int64: {
                var longGetter = (YTGetters.FromStructToLong<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(64, true))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var bigIntVector = (BigIntVector) valueVector;
                        return struct -> bigIntVector.set(bigIntVector.getValueCount(), longGetter.getLong(struct));
                    }
                };
            }
            case Uint64: {
                var longGetter = (YTGetters.FromStructToLong<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Int(64, false))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var uInt8Vector = (UInt8Vector) valueVector;
                        return struct -> uInt8Vector.set(uInt8Vector.getValueCount(), longGetter.getLong(struct));
                    }
                };
            }
            case Bool: {
                var booleanGetter = (YTGetters.FromStructToBoolean<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Bool())) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var bitVector = (BitVector) valueVector;
                        return struct -> bitVector.set(bitVector.getValueCount(), booleanGetter.getBoolean(struct) ? 1 : 0);
                    }
                };
            }
            case Float: {
                var floatGetter = (YTGetters.FromStructToFloat<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var float4Vector = (Float4Vector) valueVector;
                        return struct -> float4Vector.set(float4Vector.getValueCount(), floatGetter.getFloat(struct));
                    }
                };
            }
            case Double: {
                var doubleGetter = (YTGetters.FromStructToDouble<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var float8Vector = (Float8Vector) valueVector;
                        return struct -> float8Vector.set(float8Vector.getValueCount(), doubleGetter.getDouble(struct));
                    }
                };
            }
            case Decimal: {
                var decimalGetter = (YTGetters.FromStructToBigDecimal<Struct>) getter;
                var decimalType = (DecimalType) decimalGetter.getTiType();
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Decimal(
                        decimalType.getPrecision(), decimalType.getScale(), 128
                ))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var decimalVector = (DecimalVector) valueVector;
                        return struct ->
                                decimalVector.set(decimalVector.getValueCount(), decimalGetter.getBigDecimal(struct));
                    }
                };
            }
            case Date:
            case Date32: {
                var intGetter = (YTGetters.FromStructToInt<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Date(DateUnit.DAY))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var dateDayVector = (DateDayVector) valueVector;
                        return struct -> dateDayVector.set(dateDayVector.getValueCount(), intGetter.getInt(struct));
                    }
                };
            }
            case Datetime:
            case Datetime64: {
                var longGetter = (YTGetters.FromStructToLong<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Date(DateUnit.MILLISECOND))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var dateMilliVector = (DateMilliVector) valueVector;
                        return struct -> dateMilliVector.set(dateMilliVector.getValueCount(), longGetter.getLong(struct));
                    }
                };
            }
            case Timestamp:
            case Timestamp64: {
                var longGetter = (YTGetters.FromStructToLong<Struct>) getter;
                return new ArrowGetterFromStruct<>(field(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))) {
                    @Override
                    public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                        var timeStampMicroVector = (TimeStampMicroVector) valueVector;
                        return struct ->
                                timeStampMicroVector.set(timeStampMicroVector.getValueCount(), longGetter.getLong(struct));
                    }
                };
            }
            case List: {
                return getArrowGetterFromStruct(name, (YTGetters.FromStructToList<Struct, ?>) getter);
            }
            case Dict: {
                return getArrowGetterFromStruct(name, (YTGetters.FromStructToDict<Struct, ?, ?, ?>) getter);
            }
            case Struct: {
                return getArrowGetterFromStruct(name, (YTGetters.FromStructToStruct<Struct, ?>) getter);
            }
            default:
                return null;
        }
    }

    private <Struct, Value> ArrowGetterFromStruct<Struct> getArrowGetterFromStruct(
            String name, YTGetters.FromStructToStruct<Struct, Value> structGetter
    ) {
        var members = structGetter.getMembersGetters();
        var membersGetters = new ArrayList<ArrowGetterFromStruct<Value>>(members.size());
        for (var member : members) {
            membersGetters.add(arrowGetter(member.getKey(), member.getValue()));
        }
        return new ArrowGetterFromStruct<>(new Field(
                name, new FieldType(false, new ArrowType.Struct(), null),
                membersGetters.stream().map(member -> member.field).collect(Collectors.toList())
        )) {
            @Override
            public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                var structVector = (StructVector) valueVector;
                var membersWriters = new ArrayList<ArrowWriterFromStruct<Value>>(members.size());
                for (int i = 0; i < members.size(); i++) {
                    membersWriters.add(membersGetters.get(i).writer(structVector.getChildByOrdinal(i)));
                }
                return row -> {
                    var value = structGetter.getStruct(row);
                    structVector.setIndexDefined(structVector.getValueCount());
                    for (int i = 0; i < members.size(); i++) {
                        membersWriters.get(i).setFromStruct(value);
                    }
                };
            }
        };
    }

    private <Struct, Dict, Keys, Values> ArrowGetterFromStruct<Struct> getArrowGetterFromStruct(
            String name, YTGetters.FromStructToDict<Struct, Dict, Keys, Values> dictGetter
    ) {
        var fromDictGetter = dictGetter.getGetter();
        var keyGetter = nonComplexArrowGetter("key", fromDictGetter.getKeyGetter());
        var valueGetter = arrowGetter("value", fromDictGetter.getValueGetter());
        if (keyGetter == null) {
            return null;
        }
        return new ArrowGetterFromStruct<>(new Field(
                name, new FieldType(false, new ArrowType.Map(false), null),
                Collections.singletonList(new Field(
                        "entries", new FieldType(false, new ArrowType.Struct(), null),
                        Arrays.asList(keyGetter.field, valueGetter.field)
                ))
        )) {
            @Override
            public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                var mapVector = (MapVector) valueVector;
                var structVector = (StructVector) mapVector.getDataVector();
                var keyWriter = keyGetter.writer(structVector.getChildByOrdinal(0));
                var valueWriter = valueGetter.writer(structVector.getChildByOrdinal(1));
                return struct -> {
                    var dict = dictGetter.getDict(struct);
                    int size = fromDictGetter.getSize(dict);
                    var keys = fromDictGetter.getKeys(dict);
                    var values = fromDictGetter.getValues(dict);
                    mapVector.startNewValue(mapVector.getValueCount());
                    for (int i = 0; i < size; i++) {
                        structVector.setIndexDefined(structVector.getValueCount());
                        keyWriter.setFromList(keys, i);
                        valueWriter.setFromList(values, i);
                        structVector.setValueCount(structVector.getValueCount() + 1);
                    }
                    mapVector.endValue(mapVector.getValueCount(), size);
                };
            }
        };
    }

    private <Struct, Array> ArrowGetterFromStruct<Struct> getArrowGetterFromStruct(
            String name, YTGetters.FromStructToList<Struct, Array> listGetter
    ) {
        var elementGetter = listGetter.getElementGetter();
        var itemGetter = arrowGetter("item", elementGetter);
        return new ArrowGetterFromStruct<>(new Field(name, new FieldType(
                false, new ArrowType.List(), null
        ), Collections.singletonList(itemGetter.field))) {
            @Override
            public ArrowWriterFromStruct<Struct> writer(ValueVector valueVector) {
                var listVector = (ListVector) valueVector;
                var dataWriter = itemGetter.writer(listVector.getDataVector());
                return struct -> {
                    var list = listGetter.getList(struct);
                    int size = elementGetter.getSize(list);
                    listVector.startNewValue(listVector.getValueCount());
                    for (int i = 0; i < size; i++) {
                        dataWriter.setFromList(list, i);
                    }
                    listVector.endValue(listVector.getValueCount(), size);
                };
            }
        };
    }

    private final List<ArrowGetterFromStruct<Row>> fieldGetters;
    private final Schema schema;

    public ArrowTableRowsSerializer(List<? extends Map.Entry<String, ? extends YTGetters.FromStruct<Row>>> structsGetter) {
        super(ERowsetFormat.RF_FORMAT);
        this.serializedRows = Unpooled.buffer();
        fieldGetters = structsGetter.stream().map(memberGetter -> arrowGetter(
                memberGetter.getKey(), memberGetter.getValue()
        )).collect(Collectors.toList());
        schema = new Schema(() -> fieldGetters.stream().map(getter -> getter.field).iterator());
    }

    private static class ByteBufWritableByteChannel implements WritableByteChannel {
        private final ByteBuf buf;

        private ByteBufWritableByteChannel(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public int write(ByteBuffer src) {
            int remaining = src.remaining();
            buf.writeBytes(src);
            return remaining - src.remaining();
        }

        @Override
        public boolean isOpen() {
            return buf.isWritable();
        }

        @Override
        public void close() {
        }
    }

    @Override
    protected void writeMeta(ByteBuf buf) {
        buf.writeLongLE(serializedRows.readableBytes());
    }

    @Override
    protected int getMetaSize() {
        return Long.BYTES;
    }

    @Override
    public void write(List<Row> rows) {
        try {
            var writeChannel = new WriteChannel(new ByteBufWritableByteChannel(serializedRows));
            MessageSerializer.serialize(writeChannel, schema);
            try (
                    var allocator = ArrowUtils.rootAllocator().newChildAllocator(
                            "ArrowTableRowsSerializer", 0, Long.MAX_VALUE
                    );
                    var root = VectorSchemaRoot.create(schema, allocator)
            ) {
                var unloader = new VectorUnloader(root);
                var writers = IntStream.range(0, fieldGetters.size()).mapToObj(column -> {
                    var valueVector = root.getFieldVectors().get(column);
                    if (valueVector instanceof FixedWidthVector) {
                        ((FixedWidthVector) valueVector).allocateNew(rows.size());
                    } else {
                        valueVector.allocateNew();
                    }
                    return fieldGetters.get(column).writer(valueVector);
                }).collect(Collectors.toList());
                for (var row : rows) {
                    for (var writer : writers) {
                        writer.setFromStruct(row);
                    }
                }
                root.setRowCount(rows.size());
                try (var batch = unloader.getRecordBatch()) {
                    MessageSerializer.serialize(writeChannel, batch);
                }
                writeChannel.writeZeros(4);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
