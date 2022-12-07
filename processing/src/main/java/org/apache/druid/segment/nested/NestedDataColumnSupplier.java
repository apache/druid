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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NestedDataColumnSupplier implements Supplier<ComplexColumn>
{
  private final NestedDataColumnMetadata metadata;
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private final ImmutableBitmap nullValues;
  private final GenericIndexed<String> fields;
  private final NestedLiteralTypeInfo fieldInfo;
  private final GenericIndexed<ByteBuffer> dictionary;
  private final Supplier<FrontCodedIndexed> frontCodedDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final ColumnConfig columnConfig;
  private final SmooshedFileMapper fileMapper;

  public NestedDataColumnSupplier(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper
  )
  {
    this(bb, columnBuilder, columnConfig, jsonMapper, ColumnType.LONG.getStrategy(), ColumnType.DOUBLE.getStrategy());
  }

  // strictly for testing?
  @VisibleForTesting
  public NestedDataColumnSupplier(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper,
      TypeStrategy<Long> longTypeStrategy,
      TypeStrategy<Double> doubleTypeStrategy
  )
  {
    byte version = bb.get();

    if (version == 0x03) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        metadata = jsonMapper.readValue(
            IndexMerger.SERIALIZER_UTILS.readString(bb),
            NestedDataColumnMetadata.class
        );
        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY, mapper);
        fieldInfo = NestedLiteralTypeInfo.read(bb, fields.size());

        final ByteBuffer stringDictionaryBuffer = loadInternalFile(
            mapper,
            NestedDataColumnSerializer.STRING_DICTIONARY_FILE_NAME
        );

        final int dictionaryStartPosition = stringDictionaryBuffer.position();
        final byte dictionaryVersion = stringDictionaryBuffer.get();

        if (dictionaryVersion == EncodedStringDictionaryWriter.VERSION) {
          final byte encodingId = stringDictionaryBuffer.get();
          if (encodingId == StringEncodingStrategy.FRONT_CODED_ID) {
            frontCodedDictionarySupplier = FrontCodedIndexed.read(stringDictionaryBuffer, metadata.getByteOrder());
            dictionary = null;
          } else if (encodingId == StringEncodingStrategy.UTF8_ID) {
            // this cannot happen naturally right now since generic indexed is written in the 'legacy' format, but
            // this provides backwards compatibility should we switch at some point in the future to always
            // writing dictionaryVersion
            dictionary = GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper);
            frontCodedDictionarySupplier = null;
          } else {
            throw new ISE("impossible, unknown encoding strategy id: %s", encodingId);
          }
        } else {
          // legacy format that only supports plain utf8 enoding stored in GenericIndexed and the byte we are reading
          // as dictionaryVersion is actually also the GenericIndexed version, so we reset start position so the
          // GenericIndexed version can be correctly read
          stringDictionaryBuffer.position(dictionaryStartPosition);
          dictionary = GenericIndexed.read(stringDictionaryBuffer, GenericIndexed.UTF8_STRATEGY, mapper);
          frontCodedDictionarySupplier = null;
        }
        final ByteBuffer longDictionaryBuffer = loadInternalFile(
            mapper,
            NestedDataColumnSerializer.LONG_DICTIONARY_FILE_NAME
        );
        longDictionarySupplier = FixedIndexed.read(
            longDictionaryBuffer,
            longTypeStrategy,
            metadata.getByteOrder(),
            Long.BYTES
        );
        final ByteBuffer doubleDictionaryBuffer = loadInternalFile(
            mapper,
            NestedDataColumnSerializer.DOUBLE_DICTIONARY_FILE_NAME
        );
        doubleDictionarySupplier = FixedIndexed.read(
            doubleDictionaryBuffer,
            doubleTypeStrategy,
            metadata.getByteOrder(),
            Double.BYTES
        );
        final ByteBuffer rawBuffer = loadInternalFile(mapper, NestedDataColumnSerializer.RAW_FILE_NAME);
        compressedRawColumnSupplier = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
            NestedDataColumnSerializer.getInternalFileName(
                metadata.getFileNameBase(), NestedDataColumnSerializer.RAW_FILE_NAME
            ),
            rawBuffer,
            metadata.getByteOrder(),
            mapper
        );
        if (metadata.hasNulls()) {
          final ByteBuffer nullIndexBuffer = loadInternalFile(mapper, NestedDataColumnSerializer.NULL_BITMAP_FILE_NAME);
          nullValues = metadata.getBitmapSerdeFactory().getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap();
        }
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V3 column.");
      }
    } else {
      throw new RE("Unknown version" + version);
    }

    fileMapper = Preconditions.checkNotNull(columnBuilder.getFileMapper(), "Null fileMapper");

    this.columnConfig = columnConfig;
  }

  @Override
  public ComplexColumn get()
  {
    if (frontCodedDictionarySupplier != null) {
      return new CompressedNestedDataComplexColumn<>(
          metadata,
          columnConfig,
          compressedRawColumnSupplier,
          nullValues,
          fields,
          fieldInfo,
          frontCodedDictionarySupplier,
          longDictionarySupplier,
          doubleDictionarySupplier,
          fileMapper
      );
    }
    return new CompressedNestedDataComplexColumn<>(
        metadata,
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        dictionary::singleThreaded,
        longDictionarySupplier,
        doubleDictionarySupplier,
        fileMapper
    );
  }

  private ByteBuffer loadInternalFile(SmooshedFileMapper fileMapper, String internalFileName) throws IOException
  {
    return fileMapper.mapFile(
        NestedDataColumnSerializer.getInternalFileName(
            metadata.getFileNameBase(), internalFileName
        )
    );
  }
}
