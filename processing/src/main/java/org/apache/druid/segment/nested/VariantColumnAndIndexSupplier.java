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
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

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
        final GenericIndexed<ByteBuffer> stringDictionary;
        final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
        final Supplier<FixedIndexed<Long>> longDictionarySupplier;
        final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
        final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;

        final ByteBuffer stringDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.STRING_DICTIONARY_FILE_NAME
        );

        final int dictionaryStartPosition = stringDictionaryBuffer.position();
        final byte dictionaryVersion = stringDictionaryBuffer.get();

        if (dictionaryVersion == EncodedStringDictionaryWriter.VERSION) {
          final byte encodingId = stringDictionaryBuffer.get();
          if (encodingId == StringEncodingStrategy.FRONT_CODED_ID) {
            frontCodedStringDictionarySupplier = FrontCodedIndexed.read(
                stringDictionaryBuffer,
                byteOrder
            );
            stringDictionary = null;
          } else if (encodingId == StringEncodingStrategy.UTF8_ID) {
            // this cannot happen naturally right now since generic indexed is written in the 'legacy' format, but
            // this provides backwards compatibility should we switch at some point in the future to always
            // writing dictionaryVersion
            stringDictionary = GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper);
            frontCodedStringDictionarySupplier = null;
          } else {
            throw new ISE("impossible, unknown encoding strategy id: %s", encodingId);
          }
        } else {
          // legacy format that only supports plain utf8 enoding stored in GenericIndexed and the byte we are reading
          // as dictionaryVersion is actually also the GenericIndexed version, so we reset start position so the
          // GenericIndexed version can be correctly read
          stringDictionaryBuffer.position(dictionaryStartPosition);
          stringDictionary = GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper);
          frontCodedStringDictionarySupplier = null;
        }
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
        final ByteBuffer valueIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.BITMAP_INDEX_FILE_NAME
        );
        GenericIndexed<ImmutableBitmap> valueIndexes = GenericIndexed.read(
            valueIndexBuffer,
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
        return new VariantColumnAndIndexSupplier(
            logicalType,
            variantTypeByte,
            stringDictionary,
            frontCodedStringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            ints,
            valueIndexes,
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

  private final GenericIndexed<ByteBuffer> stringDictionary;
  private final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final Supplier<ColumnarInts> encodedValueColumnSupplier;
  @SuppressWarnings("unused")
  private final GenericIndexed<ImmutableBitmap> valueIndexes;
  private final ImmutableBitmap nullValueBitmap;

  public VariantColumnAndIndexSupplier(
      ColumnType logicalType,
      @Nullable Byte variantTypeSetByte,
      GenericIndexed<ByteBuffer> stringDictionary,
      Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      Supplier<ColumnarInts> encodedValueColumnSupplier,
      GenericIndexed<ImmutableBitmap> valueIndexes,
      @SuppressWarnings("unused") BitmapFactory bitmapFactory,
      @SuppressWarnings("unused") ColumnConfig columnConfig,
      @SuppressWarnings("unused") int numRows
  )
  {
    this.logicalType = logicalType;
    this.variantTypeSetByte = variantTypeSetByte;
    this.stringDictionary = stringDictionary;
    this.frontCodedStringDictionarySupplier = frontCodedStringDictionarySupplier;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.encodedValueColumnSupplier = encodedValueColumnSupplier;
    this.valueIndexes = valueIndexes;
    this.nullValueBitmap = valueIndexes.get(0) == null ? bitmapFactory.makeEmptyImmutableBitmap() : valueIndexes.get(0);
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new VariantColumn<>(
          frontCodedStringDictionarySupplier.get(),
          longDictionarySupplier.get(),
          doubleDictionarySupplier.get(),
          arrayDictionarySupplier.get(),
          encodedValueColumnSupplier.get(),
          nullValueBitmap,
          logicalType,
          variantTypeSetByte
      );
    }
    return new VariantColumn<>(
        stringDictionary.singleThreaded(),
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
    }
    // coming soon...
    return null;
  }
}
