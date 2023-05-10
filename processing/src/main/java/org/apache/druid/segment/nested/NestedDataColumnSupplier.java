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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
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

public class NestedDataColumnSupplier implements Supplier<NestedCommonFormatColumn>
{
  public static NestedDataColumnSupplier read(
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
        final GenericIndexed<ByteBuffer> stringDictionary;
        final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
        final Supplier<FixedIndexed<Long>> longDictionarySupplier;
        final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
        final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;

        ColumnType simpleType;
        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY, mapper);
        fieldInfo = FieldTypeInfo.read(bb, fields.size());

        if (fields.size() == 0) {
          // all nulls, in the future we'll deal with this better... but for now lets just call it a string because
          // it is the most permissive (besides json)
          simpleType = ColumnType.STRING;
        } else if (fields.size() == 1 && NestedPathFinder.JSON_PATH_ROOT.equals(fields.get(0))) {
          simpleType = fieldInfo.getTypes(0).getSingleType();
        } else {
          simpleType = null;
        }

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
        if (hasNulls) {
          columnBuilder.setHasNulls(true);
          final ByteBuffer nullIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              NestedCommonFormatColumnSerializer.NULL_BITMAP_FILE_NAME
          );
          nullValues = bitmapSerdeFactory.getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
        }

        return new NestedDataColumnSupplier(
            columnName,
            fields,
            fieldInfo,
            compressedRawColumnSupplier,
            nullValues,
            stringDictionary,
            frontCodedStringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            columnConfig,
            mapper,
            bitmapSerdeFactory,
            byteOrder,
            simpleType
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
  private final GenericIndexed<ByteBuffer> stringDictionary;
  private final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
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
      GenericIndexed<ByteBuffer> stringDictionary,
      Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
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
    this.stringDictionary = stringDictionary;
    this.frontCodedStringDictionarySupplier = frontCodedStringDictionarySupplier;
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
  public NestedCommonFormatColumn get()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new NestedDataColumnV5<>(
          columnName,
          getLogicalType(),
          columnConfig,
          compressedRawColumnSupplier,
          nullValues,
          fields,
          fieldInfo,
          frontCodedStringDictionarySupplier,
          longDictionarySupplier,
          doubleDictionarySupplier,
          arrayDictionarySupplier,
          fileMapper,
          bitmapSerdeFactory,
          byteOrder
      );
    }
    return new NestedDataColumnV5<>(
        columnName,
        getLogicalType(),
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        stringDictionary::singleThreaded,
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
}
