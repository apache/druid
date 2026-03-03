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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.serde.ColumnSerializerUtils;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public class NestedDataColumnSupplier implements Supplier<NestedCommonFormatColumn>, ColumnIndexSupplier
{
  public static NestedDataColumnSupplier read(
      ColumnType logicalType,
      boolean hasNulls,
      byte pathParserVersion,
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      NestedCommonFormatColumnFormatSpec formatSpec,
      ByteOrder byteOrder,
      NestedDataColumnSupplier parent
  )
  {
    final byte version = bb.get();
    final int columnNameLength = VByte.readInt(bb);
    final String columnName = StringUtils.fromUtf8(bb, columnNameLength);

    if (version == NestedCommonFormatColumnSerializer.V0) {
      try {
        final SegmentFileMapper mapper = columnBuilder.getFileMapper();
        final Supplier<? extends Indexed<ByteBuffer>> fieldsSupplier;
        final FieldTypeInfo fieldInfo;
        final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
        final ImmutableBitmap nullValues;
        final Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
        final Supplier<FixedIndexed<Long>> longDictionarySupplier;
        final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
        final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;


        if (parent != null) {
          fieldsSupplier = parent.fieldSupplier;
          fieldInfo = parent.fieldInfo;
          stringDictionarySupplier = parent.stringDictionarySupplier;
          longDictionarySupplier = parent.longDictionarySupplier;
          doubleDictionarySupplier = parent.doubleDictionarySupplier;
          arrayDictionarySupplier = parent.arrayDictionarySupplier;
        } else {
          if (pathParserVersion == 0x00) {
            fieldsSupplier = getAndFixFieldsSupplier(bb, byteOrder, mapper);
          } else {
            fieldsSupplier = StringEncodingStrategies.getStringDictionarySupplier(mapper, bb, byteOrder);
          }
          fieldInfo = FieldTypeInfo.read(bb, fieldsSupplier.get().size());
          final ByteBuffer stringDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              ColumnSerializerUtils.STRING_DICTIONARY_FILE_NAME
          );
          final ByteBuffer longDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              ColumnSerializerUtils.LONG_DICTIONARY_FILE_NAME
          );
          final ByteBuffer doubleDictionaryBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              ColumnSerializerUtils.DOUBLE_DICTIONARY_FILE_NAME
          );
          final ByteBuffer arrayDictionarybuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              ColumnSerializerUtils.ARRAY_DICTIONARY_FILE_NAME
          );

          stringDictionarySupplier = StringEncodingStrategies.getStringDictionarySupplier(
              mapper,
              stringDictionaryBuffer,
              byteOrder
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
          arrayDictionarySupplier = FrontCodedIntArrayIndexed.read(
              arrayDictionarybuffer,
              byteOrder
          );
        }

        final ByteBuffer rawBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
            mapper,
            columnName,
            NestedCommonFormatColumnSerializer.RAW_FILE_NAME
        );
        compressedRawColumnSupplier = rawBuffer == null
                                      ? null
                                      : CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
                                          ColumnSerializerUtils.getInternalFileName(
                                              columnName,
                                              NestedCommonFormatColumnSerializer.RAW_FILE_NAME
                                          ),
                                          rawBuffer,
                                          byteOrder,
                                          byteOrder, // byte order doesn't matter since serde is byte blobs
                                          mapper
                                      );
        if (hasNulls) {
          columnBuilder.setHasNulls(true);
          final ByteBuffer nullIndexBuffer = NestedCommonFormatColumnPartSerde.loadInternalFile(
              mapper,
              columnName,
              ColumnSerializerUtils.NULL_BITMAP_FILE_NAME
          );
          nullValues = formatSpec.getBitmapEncoding().getObjectStrategy().fromByteBufferWithSize(nullIndexBuffer);
        } else {
          nullValues = formatSpec.getBitmapEncoding().getBitmapFactory().makeEmptyImmutableBitmap();
        }

        return new NestedDataColumnSupplier(
            columnName,
            fieldsSupplier,
            fieldInfo,
            compressedRawColumnSupplier,
            nullValues,
            stringDictionarySupplier,
            longDictionarySupplier,
            doubleDictionarySupplier,
            arrayDictionarySupplier,
            columnConfig,
            mapper,
            formatSpec,
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


  @VisibleForTesting
  static Supplier<? extends Indexed<ByteBuffer>> getAndFixFieldsSupplier(
      ByteBuffer bb,
      ByteOrder byteOrder,
      SegmentFileMapper mapper
  )
  {
    final Supplier<? extends Indexed<ByteBuffer>> fieldsSupplier;
    Supplier<? extends Indexed<ByteBuffer>> _fieldsSupplier =
        StringEncodingStrategies.getStringDictionarySupplier(mapper, bb, byteOrder);
    // check for existence of bug to detect if we need a fixup adapter or not
    Indexed<ByteBuffer> fields = _fieldsSupplier.get();
    Int2ObjectMap<ByteBuffer> fixupMap = new Int2ObjectOpenHashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      String path = StringUtils.fromUtf8Nullable(fields.get(i));
      try {
        NestedPathFinder.parseJsonPath(path);
      }
      catch (DruidException d) {
        String fixed = NestedPathFinder.toNormalizedJsonPath(NestedPathFinder.parseBadJsonPath(path));
        fixupMap.put(i, StringUtils.toUtf8ByteBuffer(fixed));
      }
    }
    if (fixupMap.isEmpty()) {
      fieldsSupplier = _fieldsSupplier;
    } else {
      fieldsSupplier = () -> new FieldsFixupIndexed(_fieldsSupplier.get(), fixupMap);
    }
    return fieldsSupplier;
  }

  private final String columnName;
  private final Supplier<? extends Indexed<ByteBuffer>> fieldSupplier;
  private final FieldTypeInfo fieldInfo;
  @Nullable
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private final ImmutableBitmap nullValues;
  private final Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final ColumnConfig columnConfig;
  private final SegmentFileMapper fileMapper;
  private final NestedCommonFormatColumnFormatSpec formatSpec;
  private final ByteOrder byteOrder;

  @Nullable
  private final ColumnType simpleType;

  private NestedDataColumnSupplier(
      String columnName,
      Supplier<? extends Indexed<ByteBuffer>> fieldSupplier,
      FieldTypeInfo fieldInfo,
      @Nullable CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      Supplier<? extends Indexed<ByteBuffer>> stringDictionarySupplier,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      ColumnConfig columnConfig,
      SegmentFileMapper fileMapper,
      NestedCommonFormatColumnFormatSpec formatSpec,
      ByteOrder byteOrder,
      @Nullable ColumnType simpleType
  )
  {
    this.columnName = columnName;
    this.fieldSupplier = fieldSupplier;
    this.fieldInfo = fieldInfo;
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
    this.nullValues = nullValues;
    this.stringDictionarySupplier = stringDictionarySupplier;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.columnConfig = columnConfig;
    this.fileMapper = fileMapper;
    this.formatSpec = formatSpec;
    this.byteOrder = byteOrder;
    this.simpleType = simpleType;
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
        fieldSupplier,
        fieldInfo,
        stringDictionarySupplier,
        longDictionarySupplier,
        doubleDictionarySupplier,
        arrayDictionarySupplier,
        fileMapper,
        formatSpec,
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

  @VisibleForTesting
  public static class FieldsFixupIndexed implements Indexed<ByteBuffer>
  {
    private final Indexed<ByteBuffer> delegate;
    private final Int2ObjectMap<ByteBuffer> fixup;

    private FieldsFixupIndexed(Indexed<ByteBuffer> delegate, Int2ObjectMap<ByteBuffer> fixup)
    {
      this.delegate = delegate;
      this.fixup = fixup;
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Nullable
    @Override
    public ByteBuffer get(int index)
    {
      if (fixup.containsKey(index)) {
        return fixup.get(index).asReadOnlyBuffer();
      }
      return delegate.get(index);
    }

    @Override
    public int indexOf(@Nullable ByteBuffer value)
    {
      for (Int2ObjectMap.Entry<ByteBuffer> entry : fixup.int2ObjectEntrySet()) {
        if (entry.getValue().equals(value)) {
          return entry.getIntKey();
        }
      }
      return delegate.indexOf(value);
    }

    @Nonnull
    @Override
    public Iterator<ByteBuffer> iterator()
    {
      return new Iterator<>()
      {
        int pos = 0;
        final int size = delegate.size();
        final Iterator<ByteBuffer> delegateIterator = delegate.iterator();

        @Override
        public boolean hasNext()
        {
          return pos < size;
        }

        @Override
        public ByteBuffer next()
        {
          if (fixup.containsKey(pos)) {
            // move delegate iterator forward, but we're going to return our own value
            delegateIterator.next();
            // this is sad, but downstream stuff wants ByteBuffer, and is less sad than the original bug
            return fixup.get(pos++).asReadOnlyBuffer();
          }
          pos++;
          return delegateIterator.next();
        }
      };
    }

    @Override
    public boolean isSorted()
    {
      return delegate.isSorted();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }
  }
}
