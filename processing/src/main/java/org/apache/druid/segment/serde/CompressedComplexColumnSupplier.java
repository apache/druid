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

package org.apache.druid.segment.serde;

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.ObjectStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedComplexColumnSupplier<T> implements Supplier<ComplexColumn>
{
  public static <T> CompressedComplexColumnSupplier<T> read(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      String typeName,
      ObjectStrategy<T> objectStrategy
  )
  {
    final byte version = bb.get();

    if (version == CompressedComplexColumnSerializer.V0) {
      try {
        final ComplexColumnMetadata metadata = ColumnSerializerUtils.SMILE_MAPPER.readValue(
            IndexMerger.SERIALIZER_UTILS.readString(bb),
            ComplexColumnMetadata.class
        );
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();

        final CompressedVariableSizedBlobColumnSupplier compressedColumnSupplier;
        final ImmutableBitmap nullValues;

        final ByteBuffer fileBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            metadata.getFileNameBase(),
            CompressedComplexColumnSerializer.FILE_NAME
        );

        compressedColumnSupplier = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
            ColumnSerializerUtils.getInternalFileName(
                metadata.getFileNameBase(),
                CompressedComplexColumnSerializer.FILE_NAME
            ),
            fileBuffer,
            metadata.getByteOrder(),
            // object strategies today assume that all buffers are big endian, so we hard-code the value buffer
            // presented to the object strategy to always be big endian
            ByteOrder.BIG_ENDIAN,
            objectStrategy.readRetainsBufferReference(),
            mapper
        );

        if (metadata.hasNulls()) {
          columnBuilder.setHasNulls(true);
          final ByteBuffer nullIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              metadata.getFileNameBase(),
              ColumnSerializerUtils.NULL_BITMAP_FILE_NAME
          );
          nullValues = metadata.getBitmapSerdeFactory().getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap();
        }

        return new CompressedComplexColumnSupplier<>(typeName, objectStrategy, compressedColumnSupplier, nullValues);
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column.", version);
      }
    }
    throw new RE("Unknown version " + version);
  }

  private final String typeName;
  private final ObjectStrategy<T> objectStrategy;
  private final CompressedVariableSizedBlobColumnSupplier compressedColumnSupplier;
  private final ImmutableBitmap nullValues;

  private CompressedComplexColumnSupplier(
      String typeName,
      ObjectStrategy<T> objectStrategy,
      CompressedVariableSizedBlobColumnSupplier compressedColumnSupplier,
      ImmutableBitmap nullValues
  )
  {
    this.typeName = typeName;
    this.objectStrategy = objectStrategy;
    this.compressedColumnSupplier = compressedColumnSupplier;
    this.nullValues = nullValues;
  }

  @Override
  public ComplexColumn get()
  {
    return new CompressedComplexColumn<>(typeName, compressedColumnSupplier.get(), nullValues, objectStrategy);
  }

  public ImmutableBitmap getNullValues()
  {
    return nullValues;
  }
}
