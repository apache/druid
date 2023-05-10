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
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.IndexedStringDictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.IndexedStringDruidPredicateIndex;
import org.apache.druid.segment.column.IndexedUtf8LexicographicalRangeIndex;
import org.apache.druid.segment.column.IndexedUtf8ValueSetIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.column.StringFrontCodedDictionaryEncodedColumn;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.EncodedStringDictionaryWriter;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

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
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);


    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final SmooshedFileMapper mapper = columnBuilder.getFileMapper();
        final GenericIndexed<ByteBuffer> stringDictionary;
        final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;

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
        final int size;
        try (ColumnarInts throwAway = ints.get()) {
          size = throwAway.size();
        }
        return new ScalarStringColumnAndIndexSupplier(
            stringDictionary,
            frontCodedStringDictionarySupplier,
            ints,
            valueIndexes,
            bitmapSerdeFactory,
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



  private final GenericIndexed<ByteBuffer> stringDictionary;
  private final Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier;
  private final Supplier<ColumnarInts> encodedColumnSupplier;
  private final GenericIndexed<ImmutableBitmap> valueIndexes;
  private final ImmutableBitmap nullValueBitmap;
  private final BitmapFactory bitmapFactory;
  private final ColumnConfig columnConfig;
  private final int numRows;

  private ScalarStringColumnAndIndexSupplier(
      GenericIndexed<ByteBuffer> stringDictionary,
      Supplier<FrontCodedIndexed> frontCodedStringDictionarySupplier,
      Supplier<ColumnarInts> encodedColumnSupplier,
      GenericIndexed<ImmutableBitmap> valueIndexes,
      BitmapSerdeFactory serdeFactory,
      ColumnConfig columnConfig,
      int numRows
  )
  {
    this.stringDictionary = stringDictionary;
    this.frontCodedStringDictionarySupplier = frontCodedStringDictionarySupplier;
    this.encodedColumnSupplier = encodedColumnSupplier;
    this.valueIndexes = valueIndexes;
    this.bitmapFactory = serdeFactory.getBitmapFactory();
    this.nullValueBitmap = valueIndexes.get(0) == null ? bitmapFactory.makeEmptyImmutableBitmap() : valueIndexes.get(0);
    this.columnConfig = columnConfig;
    this.numRows = numRows;
  }

  @Override
  public NestedCommonFormatColumn get()
  {
    if (frontCodedStringDictionarySupplier != null) {
      return new StringFrontCodedDictionaryEncodedColumn(
          encodedColumnSupplier.get(),
          null,
          frontCodedStringDictionarySupplier.get()
      );
    }
    return new ScalarStringDictionaryEncodedColumn<>(encodedColumnSupplier.get(), stringDictionary.singleThreaded());
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (valueIndexes != null) {
      final Indexed<ImmutableBitmap> singleThreadedBitmaps = valueIndexes.singleThreaded();
      final Indexed<ByteBuffer> utf8Dictionary = frontCodedStringDictionarySupplier == null
                                                 ? stringDictionary.singleThreaded()
                                                 : frontCodedStringDictionarySupplier.get();
      if (clazz.equals(NullValueIndex.class)) {
        final BitmapColumnIndex nullIndex = new SimpleImmutableBitmapIndex(nullValueBitmap);
        return (T) (NullValueIndex) () -> nullIndex;
      } else if (clazz.equals(StringValueSetIndex.class)) {
        return (T) new IndexedUtf8ValueSetIndex<>(
            bitmapFactory,
            utf8Dictionary,
            singleThreadedBitmaps
        );
      } else if (clazz.equals(DruidPredicateIndex.class)) {
        return (T) new IndexedStringDruidPredicateIndex<>(
            bitmapFactory,
            new StringEncodingStrategies.Utf8ToStringIndexed(utf8Dictionary),
            singleThreadedBitmaps,
            columnConfig,
            numRows
        );
      } else if (clazz.equals(LexicographicalRangeIndex.class)) {
        return (T) new IndexedUtf8LexicographicalRangeIndex<>(
            bitmapFactory,
            utf8Dictionary,
            singleThreadedBitmaps,
            utf8Dictionary.get(0) == null,
            columnConfig,
            numRows
        );
      } else if (clazz.equals(DictionaryEncodedStringValueIndex.class)
                 || clazz.equals(DictionaryEncodedValueIndex.class)) {
        return (T) new IndexedStringDictionaryEncodedStringValueIndex<>(
            bitmapFactory,
            new StringEncodingStrategies.Utf8ToStringIndexed(utf8Dictionary),
            valueIndexes
        );
      }
    }
    return null;
  }
}
