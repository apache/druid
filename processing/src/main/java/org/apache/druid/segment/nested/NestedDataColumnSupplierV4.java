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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NestedDataColumnSupplierV4 implements Supplier<ComplexColumn>
{
  public static NestedDataColumnSupplierV4 read(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper
  )
  {
    return read(
        bb,
        columnBuilder,
        columnConfig,
        jsonMapper,
        ColumnType.LONG.getStrategy(),
        ColumnType.DOUBLE.getStrategy()
    );
  }

  public static NestedDataColumnSupplierV4 read(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper,
      TypeStrategy<Long> longTypeStrategy,
      TypeStrategy<Double> doubleTypeStrategy
  )
  {
    final byte version = bb.get();

    // v5 was never actually released, but it existed for a short time in the master branch, and doesn't hurt to be here
    if (version == 0x03 || version == 0x04 || version == 0x05) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        final NestedDataColumnMetadata metadata;
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

        metadata = jsonMapper.readValue(
            IndexMerger.SERIALIZER_UTILS.readString(bb),
            NestedDataColumnMetadata.class
        );
        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY, mapper);
        fieldInfo = FieldTypeInfo.read(bb, fields.size());

        if (fields.size() == 0) {
          // all nulls, in the future we'll deal with this better... but for now lets just call it a string because
          // it is the most permissive (besides json)
          simpleType = ColumnType.STRING;
        } else if (fields.size() == 1 &&
                   ((version == 0x03 && NestedPathFinder.JQ_PATH_ROOT.equals(fields.get(0))) ||
                    ((version == 0x04 || version == 0x05) && NestedPathFinder.JSON_PATH_ROOT.equals(fields.get(0))))
        ) {
          simpleType = fieldInfo.getTypes(0).getSingleType();
        } else {
          simpleType = null;
        }

        final ByteBuffer stringDictionaryBuffer = loadInternalFile(
            mapper,
            metadata,
            NestedDataColumnSerializerV4.STRING_DICTIONARY_FILE_NAME
        );

        final int dictionaryStartPosition = stringDictionaryBuffer.position();
        final byte dictionaryVersion = stringDictionaryBuffer.get();

        if (dictionaryVersion == EncodedStringDictionaryWriter.VERSION) {
          final byte encodingId = stringDictionaryBuffer.get();
          if (encodingId == StringEncodingStrategy.FRONT_CODED_ID) {
            frontCodedStringDictionarySupplier = FrontCodedIndexed.read(
                stringDictionaryBuffer,
                metadata.getByteOrder()
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
        final ByteBuffer longDictionaryBuffer = loadInternalFile(
            mapper,
            metadata,
            NestedDataColumnSerializerV4.LONG_DICTIONARY_FILE_NAME
        );
        longDictionarySupplier = FixedIndexed.read(
            longDictionaryBuffer,
            longTypeStrategy,
            metadata.getByteOrder(),
            Long.BYTES
        );
        final ByteBuffer doubleDictionaryBuffer = loadInternalFile(
            mapper,
            metadata,
            NestedDataColumnSerializerV4.DOUBLE_DICTIONARY_FILE_NAME
        );
        doubleDictionarySupplier = FixedIndexed.read(
            doubleDictionaryBuffer,
            doubleTypeStrategy,
            metadata.getByteOrder(),
            Double.BYTES
        );
        if (version == 0x05) {
          final ByteBuffer arrayDictionarybuffer = loadInternalFile(
              mapper,
              metadata,
              NestedDataColumnSerializerV4.ARRAY_DICTIONARY_FILE_NAME
          );
          arrayDictionarySupplier = FrontCodedIntArrayIndexed.read(
              arrayDictionarybuffer,
              metadata.getByteOrder()
          );
        } else {
          arrayDictionarySupplier = null;
        }
        final ByteBuffer rawBuffer = loadInternalFile(mapper, metadata, NestedDataColumnSerializerV4.RAW_FILE_NAME);
        compressedRawColumnSupplier = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
            NestedDataColumnSerializerV4.getInternalFileName(
                metadata.getFileNameBase(), NestedDataColumnSerializerV4.RAW_FILE_NAME
            ),
            rawBuffer,
            metadata.getByteOrder(),
            mapper
        );
        if (metadata.hasNulls()) {
          columnBuilder.setHasNulls(true);
          final ByteBuffer nullIndexBuffer = loadInternalFile(
              mapper,
              metadata,
              NestedDataColumnSerializerV4.NULL_BITMAP_FILE_NAME
          );
          nullValues = metadata.getBitmapSerdeFactory().getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap();
        }

        return new NestedDataColumnSupplierV4(
            version,
            metadata.getFileNameBase(),
            columnConfig,
            fields,
            fieldInfo,
            compressedRawColumnSupplier,
            nullValues,
            stringDictionary,
            frontCodedStringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            mapper,
            metadata.getBitmapSerdeFactory(),
            metadata.getByteOrder(),
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

  private final byte version;
  private final String columnName;
  private final ColumnConfig columnConfig;
  private final GenericIndexed<String> fields;
  private final FieldTypeInfo fieldInfo;
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private final ImmutableBitmap nullValues;
  private final GenericIndexed<ByteBuffer> stringDictionary;
  private final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final SmooshedFileMapper fileMapper;

  @Nullable
  private final ColumnType simpleType;
  private final ColumnType logicalType;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final ByteOrder byteOrder;

  private NestedDataColumnSupplierV4(
      byte version,
      String columnName,
      ColumnConfig columnConfig,
      GenericIndexed<String> fields,
      FieldTypeInfo fieldInfo,
      CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      GenericIndexed<ByteBuffer> stringDictionary,
      Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      SmooshedFileMapper fileMapper,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteOrder byteOrder,
      @Nullable ColumnType simpleType
  )
  {
    this.version = version;
    this.columnName = columnName;
    this.columnConfig = columnConfig;
    this.fields = fields;
    this.fieldInfo = fieldInfo;
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
    this.nullValues = nullValues;
    this.stringDictionary = stringDictionary;
    this.frontCodedStringDictionarySupplier = frontCodedStringDictionarySupplier;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.fileMapper = fileMapper;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.byteOrder = byteOrder;
    this.simpleType = simpleType;
    this.logicalType = simpleType == null ? ColumnType.NESTED_DATA : simpleType;
  }

  @Override
  public ComplexColumn get()
  {
    if (version == 0x03) {
      return makeV3();
    } else if (version == 0x04) {
      return makeV4();
    }
    // v5 was never actually released, but it existed for a short time in the master branch, and doesn't hurt to be here
    return makeV5();
  }

  @Nullable
  public ColumnType getSimpleType()
  {
    return simpleType;
  }

  private NestedDataColumnV3 makeV3()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new NestedDataColumnV3<>(
          columnName,
          logicalType,
          columnConfig,
          compressedRawColumnSupplier,
          nullValues,
          fields,
          fieldInfo,
          frontCodedStringDictionarySupplier,
          longDictionarySupplier,
          doubleDictionarySupplier,
          fileMapper,
          bitmapSerdeFactory,
          byteOrder
      );
    }
    return new NestedDataColumnV3<>(
        columnName,
        logicalType,
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        stringDictionary::singleThreaded,
        longDictionarySupplier,
        doubleDictionarySupplier,
        fileMapper,
        bitmapSerdeFactory,
        byteOrder
    );
  }

  private NestedDataColumnV4 makeV4()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new NestedDataColumnV4<>(
          columnName,
          logicalType,
          columnConfig,
          compressedRawColumnSupplier,
          nullValues,
          fields,
          fieldInfo,
          frontCodedStringDictionarySupplier,
          longDictionarySupplier,
          doubleDictionarySupplier,
          fileMapper,
          bitmapSerdeFactory,
          byteOrder
      );
    }
    return new NestedDataColumnV4<>(
        columnName,
        logicalType,
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        stringDictionary::singleThreaded,
        longDictionarySupplier,
        doubleDictionarySupplier,
        fileMapper,
        bitmapSerdeFactory,
        byteOrder
    );
  }

  private NestedDataColumnV5 makeV5()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new NestedDataColumnV5<>(
          columnName,
          logicalType,
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
        logicalType,
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

  private static ByteBuffer loadInternalFile(
      SmooshedFileMapper fileMapper,
      NestedDataColumnMetadata metadata,
      String internalFileName
  ) throws IOException
  {
    return fileMapper.mapFile(
        NestedDataColumnSerializerV4.getInternalFileName(metadata.getFileNameBase(), internalFileName)
    );
  }
}
