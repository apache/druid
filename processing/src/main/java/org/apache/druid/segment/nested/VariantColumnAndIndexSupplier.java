/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.nested;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.index.AllFalseBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleBitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.semantic.ArrayElementIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VariantColumnAndIndexSupplier implements Supplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static VariantColumnAndIndexSupplier read(
      ColumnType logicalType,
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    // variant types store an extra byte containing a FieldTypeInfo.TypeSet which has bits set for all types
    // present in the varaint column. this is a smaller scale, single path version of what a full nested column stores
    // for each nested path. If this value is present then the column is a mixed type and the logical type represents
    // the 'least restrictive' native Druid type, if not then all values consistently match the logical type
    final Byte variantTypeByte;
    if (bb.hasRemaining()) {
      variantTypeByte = bb.get();
    } else {
      variantTypeByte = null;
    }

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        final Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
        final Supplier<FixedIndexed<Long>> longDictionarySupplier;
        final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
        final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
        final Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier;

        final ByteBuffer stringDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.STRING_DICTIONARY_FILE_NAME
        );

        stringDictionarySupplier = StringEncodingStrategies.getStringDictionarySupplier(
            mapper,
            stringDictionaryBuffer,
            byteOrder
        );
        final ByteBuffer encodedValueColumn = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.ENCODED_VALUE_COLUMN_FILE_NAME
        );
        final CompressedVSizeColumnarIntsSupplier ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
            encodedValueColumn,
            byteOrder
        );
        final ByteBuffer longDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.LONG_DICTIONARY_FILE_NAME
        );
        final ByteBuffer doubleDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.DOUBLE_DICTIONARY_FILE_NAME
        );
        final ByteBuffer arrayElementDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.ARRAY_ELEMENT_DICTIONARY_FILE_NAME
        );
        final ByteBuffer valueIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.BITMAP_INDEX_FILE_NAME
        );
        final GenericIndexed<ImmutableBitmap> valueIndexes = GenericIndexed.read(
            valueIndexBuffer,
            bitmapSerdeFactory.getObjectStrategy(),
            columnBuilder.getFileMapper()
        );
        final ByteBuffer elementIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.ARRAY_ELEMENT_BITMAP_INDEX_FILE_NAME
        );
        final GenericIndexed<ImmutableBitmap> arrayElementIndexes = GenericIndexed.read(
            elementIndexBuffer,
            bitmapSerdeFactory.getObjectStrategy(),
            columnBuilder.getFileMapper()
        );

        longDictionarySupplier = FixedIndexed.read(
            longDictionaryBuffer,
            ColumnType.LONG.getStrategy(),
            byteOrder,
            Long.BYTES
        );
        doubleDictionarySupplier = FixedIndexed.read(
            doubleDictionaryBuffer,
            ColumnType.DOUBLE.getStrategy(),
            byteOrder,
            Double.BYTES
        );

        final ByteBuffer arrayDictionarybuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.ARRAY_DICTIONARY_FILE_NAME
        );
        arrayDictionarySupplier = FrontCodedIntArrayIndexed.read(
            arrayDictionarybuffer,
            byteOrder
        );
        final int size;
        try (ColumnarInts throwAway = ints.get()) {
          size = throwAway.size();
        }
        arrayElementDictionarySupplier = FixedIndexed.read(
            arrayElementDictionaryBuffer,
            CompressedNestedDataComplexColumn.INT_TYPE_STRATEGY,
            byteOrder,
            Integer.BYTES
        );
        return new VariantColumnAndIndexSupplier(
            logicalType,
            variantTypeByte,
            stringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            arrayElementDictionarySupplier,
            ints,
            valueIndexes,
            arrayElementIndexes,
            bitmapSerdeFactory.getBitmapFactory(),
            columnConfig,
            size
        );
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column.", version);
      }
    } else {
      throw new RE("Unknown version " + version);
    }
  }


  private final ColumnType logicalType;
  @Nullable
  private final Byte variantTypeSetByte;
  private final BitmapFactory bitmapFactory;
  private final Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier;
  private final Supplier<ColumnarInts> encodedValueColumnSupplier;
  @SuppressWarnings("unused")
  private final GenericIndexed<ImmutableBitmap> valueIndexes;
  @SuppressWarnings("unused")
  private final GenericIndexed<ImmutableBitmap> arrayElementIndexes;
  private final ImmutableBitmap nullValueBitmap;

  public VariantColumnAndIndexSupplier(
      ColumnType logicalType,
      @Nullable Byte variantTypeSetByte,
      Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier,
      Supplier<ColumnarInts> encodedValueColumnSupplier,
      GenericIndexed<ImmutableBitmap> valueIndexes,
      GenericIndexed<ImmutableBitmap> elementIndexes,
      BitmapFactory bitmapFactory,
      @SuppressWarnings("unused") ColumnConfig columnConfig,
      @SuppressWarnings("unused") int numRows
  )
  {
    this.logicalType = logicalType;
    this.variantTypeSetByte = variantTypeSetByte;
    this.stringDictionarySupplier = stringDictionarySupplier;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.arrayElementDictionarySupplier = arrayElementDictionarySupplier;
    this.encodedValueColumnSupplier = encodedValueColumnSupplier;
    this.valueIndexes = valueIndexes;
    this.arrayElementIndexes = elementIndexes;
    this.bitmapFactory = bitmapFactory;
    this.nullValueBitmap = valueIndexes.get(0) == null ? bitmapFactory.makeEmptyImmutableBitmap() : valueIndexes.get(0);
  }

  @Nullable
  public Byte getVariantTypeSetByte()
  {
    return variantTypeSetByte;
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    return new VariantColumn<>(
        stringDictionarySupplier.get(),
        longDictionarySupplier.get(),
        doubleDictionarySupplier.get(),
        arrayDictionarySupplier.get(),
        encodedValueColumnSupplier.get(),
        nullValueBitmap,
        logicalType,
        variantTypeSetByte
    );
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      final BitmapColumnIndex nullIndex = new SimpleImmutableBitmapIndex(nullValueBitmap);
      return (T) (NullValueIndex) () -> nullIndex;
    } else if (clazz.equals(ValueIndexes.class) && variantTypeSetByte == null && logicalType.isArray()) {
      return (T) new ArrayValueIndexes();
    } else if (clazz.equals(ArrayElementIndexes.class) && variantTypeSetByte == null && logicalType.isArray()) {
      return (T) new VariantArrayElementIndexes();
    }
    return null;
  }

  private ImmutableBitmap getBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }

    final ImmutableBitmap bitmap = valueIndexes.get(idx);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  private ImmutableBitmap getElementBitmap(int idx)
  {
    if (idx < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }
    final int elementDictionaryIndex = arrayElementDictionarySupplier.get().indexOf(idx);
    if (elementDictionaryIndex < 0) {
      return bitmapFactory.makeEmptyImmutableBitmap();
    }
    final ImmutableBitmap bitmap = arrayElementIndexes.get(elementDictionaryIndex);
    return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
  }

  private class ArrayValueIndexes implements ValueIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex forValue(@Nonnull Object value, TypeSignature<ValueType> valueType)
    {
      if (!valueType.isArray()) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(valueType), value);
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(
          eval,
          ExpressionType.fromColumnTypeStrict(logicalType)
      );
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final Object[] arrayToMatch = castForComparison.asArray();
      Indexed elements;
      final int elementOffset;
      switch (logicalType.getElementType().getType()) {
        case STRING:
          elements = stringDictionarySupplier.get();
          elementOffset = 0;
          break;
        case LONG:
          elements = longDictionarySupplier.get();
          elementOffset = stringDictionarySupplier.get().size();
          break;
        case DOUBLE:
          elements = doubleDictionarySupplier.get();
          elementOffset = stringDictionarySupplier.get().size() + longDictionarySupplier.get().size();
          break;
        default:
          throw DruidException.defensive(
              "Unhandled array type [%s] how did this happen?",
              logicalType.getElementType()
          );
      }

      final int[] ids = new int[arrayToMatch.length];
      final int arrayOffset = stringDictionarySupplier.get().size() + longDictionarySupplier.get().size() + doubleDictionarySupplier.get().size();
      for (int i = 0; i < arrayToMatch.length; i++) {
        if (arrayToMatch[i] == null) {
          ids[i] = 0;
        } else if (logicalType.getElementType().is(ValueType.STRING)) {
          ids[i] = elements.indexOf(StringUtils.toUtf8ByteBuffer((String) arrayToMatch[i]));
        } else {
          ids[i] = elements.indexOf(arrayToMatch[i]) + elementOffset;
        }
        if (ids[i] < 0) {
          if (value == null) {
            return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
          }
        }
      }

      final FrontCodedIntArrayIndexed dictionary = arrayDictionarySupplier.get();
      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int localId = dictionary.indexOf(ids);
          if (includeUnknown) {
            if (localId < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getBitmap(localId + arrayOffset), nullValueBitmap)
            );
          }
          if (localId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getBitmap(localId + arrayOffset));
        }
      };
    }
  }

  private class VariantArrayElementIndexes implements ArrayElementIndexes
  {
    @Nullable
    @Override
    public BitmapColumnIndex containsValue(@Nullable Object value, TypeSignature<ValueType> elementValueType)
    {
      // this column doesn't store nested arrays, bail out if checking if we contain an array
      if (elementValueType.isArray()) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final ExprEval<?> eval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(elementValueType), value);

      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(
          eval,
          ExpressionType.fromColumnTypeStrict(logicalType.isArray() ? logicalType.getElementType() : logicalType)
      );
      if (castForComparison == null) {
        return new AllFalseBitmapColumnIndex(bitmapFactory, nullValueBitmap);
      }
      final Indexed elements;
      final int elementOffset;
      switch (logicalType.getElementType().getType()) {
        case STRING:
          elements = stringDictionarySupplier.get();
          elementOffset = 0;
          break;
        case LONG:
          elements = longDictionarySupplier.get();
          elementOffset = stringDictionarySupplier.get().size();
          break;
        case DOUBLE:
          elements = doubleDictionarySupplier.get();
          elementOffset = stringDictionarySupplier.get().size() + longDictionarySupplier.get().size();
          break;
        default:
          throw DruidException.defensive(
              "Unhandled array type [%s] how did this happen?",
              logicalType.getElementType()
          );
      }

      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int elementId = getElementId();
          if (includeUnknown) {
            if (elementId < 0) {
              return bitmapResultFactory.wrapDimensionValue(nullValueBitmap);
            }
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getElementBitmap(elementId), nullValueBitmap)
            );
          }
          if (elementId < 0) {
            return bitmapResultFactory.wrapDimensionValue(bitmapFactory.makeEmptyImmutableBitmap());
          }
          return bitmapResultFactory.wrapDimensionValue(getElementBitmap(elementId));
        }

        private int getElementId()
        {
          if (castForComparison.value() == null) {
            return 0;
          } else if (castForComparison.type().is(ExprType.STRING)) {
            return elements.indexOf(StringUtils.toUtf8ByteBuffer(castForComparison.asString()));
          } else {
            return elements.indexOf(castForComparison.value()) + elementOffset;
          }
        }
      };
    }
  }
}
