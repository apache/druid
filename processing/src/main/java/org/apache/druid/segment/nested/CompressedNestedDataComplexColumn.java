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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarDoublesSuppliers;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumn;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link NestedDataComplexColumn} which uses a {@link CompressedVariableSizedBlobColumn} for the
 * 'raw' {@link StructuredData} values and provides selectors for nested 'literal' field columns.
 */
public final class CompressedNestedDataComplexColumn extends NestedDataComplexColumn
{
  private final NestedDataColumnMetadata metadata;
  private final Closer closer;
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private CompressedVariableSizedBlobColumn compressedRawColumn;
  private final ImmutableBitmap nullValues;

  private final GenericIndexed<String> fields;
  private final NestedLiteralTypeInfo fieldInfo;

  private final GenericIndexed<String> stringDictionary;
  private final FixedIndexed<Long> longDictionary;
  private final FixedIndexed<Double> doubleDictionary;
  private final SmooshedFileMapper fileMapper;

  private final ConcurrentHashMap<String, ColumnHolder> columns = new ConcurrentHashMap<>();

  private static final ObjectStrategy<Object> STRATEGY = NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy();

  public CompressedNestedDataComplexColumn(
      NestedDataColumnMetadata metadata,
      @SuppressWarnings("unused") ColumnConfig columnConfig,
      CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      GenericIndexed<String> fields,
      NestedLiteralTypeInfo fieldInfo,
      GenericIndexed<String> stringDictionary,
      FixedIndexed<Long> longDictionary,
      FixedIndexed<Double> doubleDictionary,
      SmooshedFileMapper fileMapper
  )
  {
    this.metadata = metadata;
    this.nullValues = nullValues;
    this.fields = fields;
    this.fieldInfo = fieldInfo;
    this.stringDictionary = stringDictionary;
    this.longDictionary = longDictionary;
    this.doubleDictionary = doubleDictionary;
    this.fileMapper = fileMapper;
    this.closer = Closer.create();
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
  }

  public GenericIndexed<String> getFields()
  {
    return fields;
  }

  public NestedLiteralTypeInfo getFieldInfo()
  {
    return fieldInfo;
  }

  public GenericIndexed<String> getStringDictionary()
  {
    return stringDictionary;
  }

  public FixedIndexed<Long> getLongDictionary()
  {
    return longDictionary;
  }

  public FixedIndexed<Double> getDoubleDictionary()
  {
    return doubleDictionary;
  }

  @Nullable
  @Override
  public Object getRowValue(int rowNum)
  {
    if (nullValues.get(rowNum)) {
      return null;
    }

    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }

    final ByteBuffer valueBuffer = compressedRawColumn.get(rowNum);
    return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        if (nullValues.get(offset.getOffset())) {
          return null;
        }
        final ByteBuffer valueBuffer = compressedRawColumn.get(offset.getOffset());
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      }

      @Override
      public Class classOfObject()
      {
        return getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", CompressedNestedDataComplexColumn.this);
      }
    };
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (compressedRawColumn == null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }
    return new VectorObjectSelector()
    {
      final Object[] vector = new Object[offset.getMaxVectorSize()];

      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          final int startOffset = offset.getStartOffset();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getForOffset(startOffset + i);
          }
        } else {
          final int[] offsets = offset.getOffsets();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getForOffset(offsets[i]);

          }
        }

        id = offset.getId();
        return vector;
      }

      @Nullable
      private Object getForOffset(int offset)
      {
        if (nullValues.get(offset)) {
          // maybe someday can use bitmap batch operations for nulls?
          return null;
        }
        final ByteBuffer valueBuffer = compressedRawColumn.get(offset);
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    };
  }


  @Override
  public int getLength()
  {
    return 0;
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  @Override
  public DimensionSelector makeDimensionSelector(List<NestedPathPart> path, ReadableOffset readableOffset, ExtractionFn fn)
  {
    final String field = getField(path);
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) getColumnHolder(field).getColumn();
      return col.makeDimensionSelector(readableOffset, fn);
    } else {
      return DimensionSelector.constant(null);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(List<NestedPathPart> path, ReadableOffset readableOffset)
  {
    final String field = getField(path);
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = getColumnHolder(field).getColumn();
      return col.makeColumnValueSelector(readableOffset);
    } else {
      return NilColumnValueSelector.instance();
    }
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  )
  {
    final String field = getField(path);
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) getColumnHolder(field).getColumn();
      return col.makeSingleValueDimensionVectorSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(List<NestedPathPart> path, ReadableVectorOffset readableOffset)
  {
    final String field = getField(path);
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = getColumnHolder(field).getColumn();
      return col.makeVectorObjectSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(List<NestedPathPart> path, ReadableVectorOffset readableOffset)
  {
    final String field = getField(path);
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = getColumnHolder(field).getColumn();
      return col.makeVectorValueSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path)
  {
    final String field = getField(path);
    if (fields.indexOf(field) < 0) {
      return null;
    }
    return getColumnHolder(field).getIndexSupplier();
  }

  @Override
  public boolean isNumeric(List<NestedPathPart> path)
  {
    final String field = getField(path);
    if (fields.indexOf(field) < 0) {
      return true;
    }
    return getColumnHolder(field).getCapabilities().isNumeric();
  }

  private String getField(List<NestedPathPart> path)
  {
    return NestedPathFinder.toNormalizedJqPath(path);
  }

  private ColumnHolder getColumnHolder(String field)
  {
    return columns.computeIfAbsent(field, this::readNestedFieldColumn);
  }

  private ColumnHolder readNestedFieldColumn(String field)
  {
    try {
      if (fields.indexOf(field) < 0) {
        return null;
      }
      final NestedLiteralTypeInfo.TypeSet types = fieldInfo.getTypes(fields.indexOf(field));
      final ByteBuffer dataBuffer = fileMapper.mapFile(
          NestedDataColumnSerializer.getFieldFileName(metadata.getFileNameBase(), field)
      );
      if (dataBuffer == null) {
        throw new ISE("Can't find field [%s] in [%s] file.", field, metadata.getFileNameBase());
      }

      ColumnBuilder columnBuilder = new ColumnBuilder().setFileMapper(fileMapper);
      // heh, maybe this should be its own class, or DictionaryEncodedColumnPartSerde could be cooler
      DictionaryEncodedColumnPartSerde.VERSION version = DictionaryEncodedColumnPartSerde.VERSION.fromByte(
          dataBuffer.get()
      );
      // we should check this someday soon, but for now just read it to push the buffer position ahead
      int flags = dataBuffer.getInt();
      Preconditions.checkState(
          flags == DictionaryEncodedColumnPartSerde.NO_FLAGS,
          StringUtils.format(
              "Unrecognized bits set in space reserved for future flags for field column [%s]",
              field
          )
      );

      final FixedIndexed<Integer> localDictionary = FixedIndexed.read(
          dataBuffer,
          NestedDataColumnSerializer.INT_TYPE_STRATEGY,
          metadata.getByteOrder(),
          Integer.BYTES
      );
      ByteBuffer bb = dataBuffer.asReadOnlyBuffer().order(metadata.getByteOrder());
      int longsLength = bb.getInt();
      int doublesLength = bb.getInt();
      dataBuffer.position(dataBuffer.position() + Integer.BYTES + Integer.BYTES);
      int pos = dataBuffer.position();
      final Supplier<ColumnarLongs> longs = longsLength > 0 ? CompressedColumnarLongsSupplier.fromByteBuffer(dataBuffer, metadata.getByteOrder()) : () -> null;
      dataBuffer.position(pos + longsLength);
      pos = dataBuffer.position();
      final Supplier<ColumnarDoubles> doubles = doublesLength > 0 ? CompressedColumnarDoublesSuppliers.fromByteBuffer(dataBuffer, metadata.getByteOrder()) : () -> null;
      dataBuffer.position(pos + doublesLength);
      final WritableSupplier<ColumnarInts> ints;
      if (version == DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED) {
        ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(dataBuffer, metadata.getByteOrder());
      } else {
        ints = VSizeColumnarInts.readFromByteBuffer(dataBuffer);
      }
      ColumnType theType = types.getSingleType();
      columnBuilder.setType(theType == null ? ValueType.STRING : theType.getType());

      GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
          dataBuffer,
          metadata.getBitmapSerdeFactory().getObjectStrategy(),
          columnBuilder.getFileMapper()
      );
      Supplier<DictionaryEncodedColumn<?>> columnSupplier = () ->
          closer.register(new NestedFieldLiteralDictionaryEncodedColumn(
              types,
              longs.get(),
              doubles.get(),
              ints.get(),
              stringDictionary,
              longDictionary,
              doubleDictionary,
              localDictionary,
              localDictionary.get(0) == 0
              ? rBitmaps.get(0)
              : metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap()
          ));
      columnBuilder.setHasMultipleValues(false)
                   .setHasNulls(true)
                   .setDictionaryEncodedColumnSupplier(columnSupplier);
      columnBuilder.setIndexSupplier(
          new NestedFieldLiteralColumnIndexSupplier(
              types,
              metadata.getBitmapSerdeFactory().getBitmapFactory(),
              rBitmaps,
              localDictionary,
              stringDictionary,
              longDictionary,
              doubleDictionary
          ),
          true,
          false
      );
      return columnBuilder.build();
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to read data for [%s]", field);
    }
  }
}
