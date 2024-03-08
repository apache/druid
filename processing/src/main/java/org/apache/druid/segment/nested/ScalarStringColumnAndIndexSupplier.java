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
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;
import org.apache.druid.segment.serde.StringUtf8ColumnIndexSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ScalarStringColumnAndIndexSupplier implements Supplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static ScalarStringColumnAndIndexSupplier read(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteBuffer bb,
      ColumnBuilder columnBuilder
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        final Supplier<? extends Indexed<ByteBuffer>> dictionarySupplier;

        final ByteBuffer stringDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.STRING_DICTIONARY_FILE_NAME
        );

        dictionarySupplier = StringEncodingStrategies.getStringDictionarySupplier(
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
        return new ScalarStringColumnAndIndexSupplier(
            dictionarySupplier,
            ints,
            valueIndexes,
            bitmapSerdeFactory
        );
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V%s column.", version);
      }
    } else {
      throw new RE("Unknown version " + version);
    }
  }

  private final Supplier<? extends Indexed<ByteBuffer>> dictionarySupplier;
  private final Supplier<ColumnarInts> encodedColumnSupplier;
  private final GenericIndexed<ImmutableBitmap> valueIndexes;
  private final ColumnIndexSupplier stringIndexSupplier;

  private ScalarStringColumnAndIndexSupplier(
      Supplier<? extends Indexed<ByteBuffer>> dictionarySupplier,
      Supplier<ColumnarInts> encodedColumnSupplier,
      GenericIndexed<ImmutableBitmap> valueIndexes,
      BitmapSerdeFactory serdeFactory
  )
  {
    this.dictionarySupplier = dictionarySupplier;
    this.encodedColumnSupplier = encodedColumnSupplier;
    this.valueIndexes = valueIndexes;
    this.stringIndexSupplier = new StringUtf8ColumnIndexSupplier<>(
        serdeFactory.getBitmapFactory(),
        dictionarySupplier,
        valueIndexes,
        null
    );
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    return new StringUtf8DictionaryEncodedColumn(encodedColumnSupplier.get(), null, dictionarySupplier.get());
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (valueIndexes != null) {
      return stringIndexSupplier.as(clazz);
    }
    return null;
  }
}
