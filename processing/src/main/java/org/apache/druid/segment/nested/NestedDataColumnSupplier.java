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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnPartSupplier;
import org.apache.druid.segment.column.ColumnSize;
import org.apache.druid.segment.column.ColumnSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class NestedDataColumnSupplier implements ColumnSupplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static NestedDataColumnSupplier read(
      ColumnType logicalType,
      boolean hasNulls,
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteOrder byteOrder
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        final GenericIndexed<String> fields;
        final FieldTypeInfo fieldInfo;
        final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
        final ImmutableBitmap nullValues;
        final ColumnPartSupplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
        final ColumnPartSupplier<FixedIndexed<Long>> longDictionarySupplier;
        final ColumnPartSupplier<FixedIndexed<Double>> doubleDictionarySupplier;
        final ColumnPartSupplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;

        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY, mapper);
        fieldInfo = FieldTypeInfo.read(bb, fields.size());

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

        final ByteBuffer longDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.LONG_DICTIONARY_FILE_NAME
        );
        longDictionarySupplier = FixedIndexed.read(
            longDictionaryBuffer,
            ColumnType.LONG.getStrategy(),
            byteOrder,
            Long.BYTES
        );
        final ByteBuffer doubleDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.DOUBLE_DICTIONARY_FILE_NAME
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
        final ByteBuffer rawBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.RAW_FILE_NAME
        );
        compressedRawColumnSupplier = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
            NestedCommonFormatColumnSerializer.getInternalFileName(
                columnName,
                NestedCommonFormatColumnSerializer.RAW_FILE_NAME
            ),
            rawBuffer,
            byteOrder,
            mapper
        );
        final int nullValueBitmapSize;
        if (hasNulls) {
          columnBuilder.setHasNulls(true);
          final ByteBuffer nullIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              NestedCommonFormatColumnSerializer.NULL_BITMAP_FILE_NAME
          );
          nullValueBitmapSize = nullIndexBuffer.remaining();
          nullValues = bitmapSerdeFactory.getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
          nullValueBitmapSize = 0;
        }

        return new NestedDataColumnSupplier(
            columnName,
            fields,
            fieldInfo,
            compressedRawColumnSupplier,
            nullValues,
            nullValueBitmapSize,
            stringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            columnConfig,
            mapper,
            bitmapSerdeFactory,
            byteOrder,
            logicalType
        );
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column.", version);
      }
    } else {
      throw new RE("Unknown version " + version);
    }
  }

  private final String columnName;
  private final GenericIndexed<String> fields;
  private final FieldTypeInfo fieldInfo;
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private final ImmutableBitmap nullValues;
  private final int nullValueBitmapSize;
  private final ColumnPartSupplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
  private final ColumnPartSupplier<FixedIndexed<Long>> longDictionarySupplier;
  private final ColumnPartSupplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final ColumnPartSupplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final ColumnConfig columnConfig;
  private final SmooshedFileMapper fileMapper;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final ByteOrder byteOrder;

  @Nullable
  private final ColumnType simpleType;

  private NestedDataColumnSupplier(
      String columnName,
      GenericIndexed<String> fields,
      FieldTypeInfo fieldInfo,
      CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      int nullValueBitmapSize,
      ColumnPartSupplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier,
      ColumnPartSupplier<FixedIndexed<Long>> longDictionarySupplier,
      ColumnPartSupplier<FixedIndexed<Double>> doubleDictionarySupplier,
      ColumnPartSupplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      ColumnConfig columnConfig,
      SmooshedFileMapper fileMapper,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteOrder byteOrder,
      @Nullable ColumnType simpleType
  )
  {
    this.columnName = columnName;
    this.fields = fields;
    this.fieldInfo = fieldInfo;
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
    this.nullValues = nullValues;
    this.nullValueBitmapSize = nullValueBitmapSize;
    this.stringDictionarySupplier = stringDictionarySupplier;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.columnConfig = columnConfig;
    this.fileMapper = fileMapper;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.byteOrder = byteOrder;
    this.simpleType = simpleType;
  }

  @Override
  public Map<String, ColumnPartSize> getComponents()
  {
    LinkedHashMap<String, ColumnPartSize> fieldsParts = new LinkedHashMap<>();
    try (NestedDataColumnV5 column = (NestedDataColumnV5) get()) {
      for (String field : fields) {
        ColumnHolder subHolder = column.getColumnHolder(NestedPathFinder.parseJsonPath(field));
        fieldsParts.put(
            field,
            new ColumnPartSize("nestedColumn", 0, subHolder.getColumnSize().getComponents())
        );
      }
    }
    return ImmutableMap.of(
        "fieldsList", fields.getColumnPartSize(),
        ColumnSize.DATA_SECTION, compressedRawColumnSupplier.getColumnPartSize(),
        ColumnSize.STRING_VALUE_DICTIONARY_COLUMN_PART, stringDictionarySupplier.getColumnPartSize(),
        ColumnSize.LONG_VALUE_DICTIONARY_COLUMN_PART, longDictionarySupplier.getColumnPartSize(),
        ColumnSize.DOUBLE_VALUE_DICTIONARY_COLUMN_PART, doubleDictionarySupplier.getColumnPartSize(),
        ColumnSize.ARRAY_VALUE_DICTIONARY_COLUMN_PART, arrayDictionarySupplier.getColumnPartSize(),
        "nestedColumns", new ColumnPartSize("nestedColumns", 0, fieldsParts)
    );
  }

  @Override
  public Map<String, ColumnPartSize> getIndexComponents()
  {
    LinkedHashMap<String, ColumnPartSize> fieldsIndexes = new LinkedHashMap<>();
    try (NestedDataColumnV5 column = (NestedDataColumnV5) get()) {
      for (String field : fields) {
        ColumnIndexSupplier indexSupplier = column.getColumnIndexSupplier(NestedPathFinder.parseJsonPath(field));
        fieldsIndexes.put(
            field,
            new ColumnPartSize("nestedColumn", 0, indexSupplier.getIndexComponents())
        );
      }
    }
    if (nullValueBitmapSize > 0) {
      return ImmutableMap.of(
          ColumnSize.NULL_VALUE_INDEX_COLUMN_PART, ColumnPartSize.simple("nullBitmap", nullValueBitmapSize)
      );
    }
    return Collections.emptyMap();
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    return new NestedDataColumnV5<>(
        columnName,
        getLogicalType(),
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        stringDictionarySupplier,
        longDictionarySupplier,
        doubleDictionarySupplier,
        arrayDictionarySupplier,
        fileMapper,
        bitmapSerdeFactory,
        byteOrder
    );
  }

  public ColumnType getLogicalType()
  {
    return simpleType == null ? ColumnType.NESTED_DATA : simpleType;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      return (T) (NullValueIndex) () -> new SimpleImmutableBitmapIndex(nullValues);
    }
    return null;
  }
}
