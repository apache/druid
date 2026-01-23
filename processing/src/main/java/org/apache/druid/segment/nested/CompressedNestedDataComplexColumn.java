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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.semantic.SemanticUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.BitmapIndexType;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.AtomicIntegerReadableOffset;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarDoublesSuppliers;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumn;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of {@link NestedDataComplexColumn} which uses a {@link CompressedVariableSizedBlobColumn} for the
 * 'raw' {@link StructuredData} values and provides selectors for nested field columns specified by ordered lists of
 * {@link NestedPathPart}.
 * <p>
 * The list of available nested paths is stored in {@link #fieldsSupplier}, and their associated types stored in
 * {@link #fieldInfo} which can be accessed by the index of the field in {@link #fieldsSupplier}.
 * <p>
 * In the case that the nested column has only a single field, and that field is the 'root' path, specified by
 * {@link #rootFieldPath}, the selectors created for the complex column itself will use the 'root' path selectors
 * instead.
 */
public abstract class CompressedNestedDataComplexColumn<TKeyDictionary extends Indexed<ByteBuffer>, TStringDictionary extends Indexed<ByteBuffer>>
    extends NestedDataComplexColumn implements NestedCommonFormatColumn
{
  private static final Map<Class<?>, Function<CompressedNestedDataComplexColumn, ?>> AS_MAP =
      SemanticUtils.makeAsMap(CompressedNestedDataComplexColumn.class);

  private static final ObjectStrategy<Object> STRATEGY = NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy();
  public static final IntTypeStrategy INT_TYPE_STRATEGY = new IntTypeStrategy();
  private final ColumnConfig columnConfig;
  private final Closer closer;
  @Nullable
  private final CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier;
  private final ImmutableBitmap nullValues;
  private final Supplier<TKeyDictionary> fieldsSupplier;
  private final FieldTypeInfo fieldInfo;
  private final Supplier<TStringDictionary> stringDictionarySupplier;
  private final Supplier<FixedIndexed<Long>> longDictionarySupplier;
  private final Supplier<FixedIndexed<Double>> doubleDictionarySupplier;
  @Nullable
  private final Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier;
  private final SegmentFileMapper fileMapper;
  private final String rootFieldPath;
  private final ColumnType logicalType;
  private final String columnName;
  private final NestedCommonFormatColumnFormatSpec formatSpec;
  private final ByteOrder byteOrder;
  private final ConcurrentHashMap<Integer, BaseColumnHolder> columns = new ConcurrentHashMap<>();
  private CompressedVariableSizedBlobColumn compressedRawColumn;

  public CompressedNestedDataComplexColumn(
      String columnName,
      ColumnType logicalType,
      @SuppressWarnings("unused") ColumnConfig columnConfig,
      @Nullable CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      Supplier<TKeyDictionary> fieldsSupplier,
      FieldTypeInfo fieldInfo,
      Supplier<TStringDictionary> stringDictionary,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      @Nullable Supplier<FrontCodedIntArrayIndexed> arrayDictionarySupplier,
      SegmentFileMapper fileMapper,
      NestedCommonFormatColumnFormatSpec formatSpec,
      ByteOrder byteOrder,
      String rootFieldPath
  )
  {
    this.columnName = columnName;
    this.logicalType = logicalType;
    this.nullValues = nullValues;
    this.fieldsSupplier = fieldsSupplier;
    this.fieldInfo = fieldInfo;
    this.stringDictionarySupplier = stringDictionary;
    this.longDictionarySupplier = longDictionarySupplier;
    this.doubleDictionarySupplier = doubleDictionarySupplier;
    this.arrayDictionarySupplier = arrayDictionarySupplier;
    this.fileMapper = fileMapper;
    this.closer = Closer.create();
    this.compressedRawColumnSupplier = compressedRawColumnSupplier;
    this.formatSpec = formatSpec;
    this.byteOrder = byteOrder;
    this.rootFieldPath = rootFieldPath;
    this.columnConfig = columnConfig;
  }

  public abstract List<NestedPathPart> parsePath(String path);

  public abstract String getField(List<NestedPathPart> path);

  public abstract String getFieldFileName(String fileNameBase, String field, int fieldIndex);

  @Override
  public SortedMap<String, FieldTypeInfo.MutableTypeSet> getFieldTypeInfo()
  {
    SortedMap<String, FieldTypeInfo.MutableTypeSet> fieldMap = new TreeMap<>();
    for (NestedField field : getAllNestedFields()) {
      FieldTypeInfo.TypeSet types = fieldInfo.getTypes(field.fieldIndex);
      fieldMap.put(field.fieldName, new FieldTypeInfo.MutableTypeSet(types.getByteValue()));
    }
    return fieldMap;
  }

  @Override
  public ColumnType getLogicalType()
  {
    return logicalType;
  }

  @Override
  public List<List<NestedPathPart>> getNestedFields()
  {
    return getAllParsedNestedFields().stream().map(pair -> pair.rhs).collect(Collectors.toList());
  }

  public TStringDictionary getUtf8BytesDictionary()
  {
    return stringDictionarySupplier.get();
  }

  @Override
  public Indexed<String> getStringDictionary()
  {
    return new StringEncodingStrategies.Utf8ToStringIndexed(stringDictionarySupplier.get());
  }

  @Override
  public Indexed<Long> getLongDictionary()
  {
    return longDictionarySupplier.get();
  }

  @Override
  public Indexed<Double> getDoubleDictionary()
  {
    return doubleDictionarySupplier.get();
  }

  @Override
  public Indexed<Object[]> getArrayDictionary()
  {
    if (arrayDictionarySupplier == null) {
      return Indexed.empty();
    }
    Iterable<Object[]> arrays = () -> {
      final TStringDictionary stringDictionary = stringDictionarySupplier.get();
      final FixedIndexed<Long> longDictionary = longDictionarySupplier.get();
      final FixedIndexed<Double> doubleDictionary = doubleDictionarySupplier.get();

      return new Iterator<>()
      {
        final Iterator<int[]> delegate = arrayDictionarySupplier.get().iterator();

        @Override
        public boolean hasNext()
        {
          return delegate.hasNext();
        }

        @Override
        public Object[] next()
        {
          final int[] next = delegate.next();
          final Object[] nextArray = new Object[next.length];
          for (int i = 0; i < nextArray.length; i++) {
            nextArray[i] = lookupId(next[i]);
          }
          return nextArray;
        }

        private Object lookupId(int globalId)
        {
          if (globalId == 0) {
            return null;
          }
          final int adjustLongId = stringDictionary.size();
          final int adjustDoubleId = stringDictionary.size() + longDictionary.size();
          if (globalId < adjustLongId) {
            return StringUtils.fromUtf8Nullable(stringDictionary.get(globalId));
          } else if (globalId < adjustDoubleId) {
            return longDictionary.get(globalId - adjustLongId);
          } else if (globalId < adjustDoubleId + doubleDictionary.size()) {
            return doubleDictionary.get(globalId - adjustDoubleId);
          }
          throw new IAE("Unknown globalId [%s]", globalId);
        }
      };
    };
    return new Indexed<>()
    {
      @Override
      public int size()
      {
        return arrayDictionarySupplier.get().size();
      }

      @Nullable
      @Override
      public Object[] get(int index)
      {
        throw new UnsupportedOperationException("get not supported");
      }

      @Override
      public int indexOf(@Nullable Object[] value)
      {
        throw new UnsupportedOperationException("indexOf not supported");
      }

      @Override
      public Iterator<Object[]> iterator()
      {
        return arrays.iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // meh
      }
    };
  }

  public ImmutableBitmap getNullValues()
  {
    return nullValues;
  }

  @Override
  @Nullable
  public Object getRowValue(int rowNum)
  {
    if (nullValues.get(rowNum)) {
      return null;
    }

    if (compressedRawColumn == null && compressedRawColumnSupplier != null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }

    if (compressedRawColumnSupplier != null) {
      final ByteBuffer valueBuffer = compressedRawColumn.get(rowNum);
      return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
    }

    final List<StructuredDataBuilder.Element> elements = getAllParsedNestedFields()
        .stream()
        .map(pair -> {
          NestedFieldDictionaryEncodedColumn column = (NestedFieldDictionaryEncodedColumn) getColumnHolder(
              pair.lhs.fieldName,
              pair.lhs.fieldIndex
          ).getColumn();
          return StructuredDataBuilder.Element.of(pair.rhs, column.lookupObject(column.getSingleValueRow(rowNum)));
        })
        .collect(Collectors.toList());
    return new StructuredDataBuilder(elements).build();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    List<NestedField> allFields = getAllNestedFields();
    if (!logicalType.equals(ColumnType.NESTED_DATA)
        && allFields.size() == 1
        && rootFieldPath.equals(Iterables.getOnlyElement(allFields).fieldName)) {
      return makeColumnValueSelector(
          ImmutableList.of(),
          null /* not used */,
          offset
      );
    }
    final Supplier<Object> valueProvider;
    if (compressedRawColumnSupplier != null) {
      if (compressedRawColumn == null) {
        compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
      }
      valueProvider = () -> {
        final ByteBuffer valueBuffer = compressedRawColumn.get(offset.getOffset());
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      };
    } else {
      List<Pair<List<NestedPathPart>, ? extends ColumnValueSelector>> fieldSelectors =
          getAllParsedNestedFields().stream()
                                    .map(pair -> Pair.of(
                                        pair.rhs,
                                        getColumnHolder(
                                            pair.lhs.fieldName,
                                            pair.lhs.fieldIndex
                                        ).getColumn().makeColumnValueSelector(offset)
                                    ))
                                    .collect(Collectors.toList());
      valueProvider = () -> {
        List<StructuredDataBuilder.Element> elements = fieldSelectors
            .stream()
            .map(c -> StructuredDataBuilder.Element.of(c.lhs, c.rhs.getObject()))
            .collect(Collectors.toList());
        return new StructuredDataBuilder(elements).build();
      };
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
        return valueProvider.get();
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
    List<Pair<NestedField, List<NestedPathPart>>> allFields = getAllParsedNestedFields();
    if (!logicalType.equals(ColumnType.NESTED_DATA)
        && allFields.size() == 1
        && rootFieldPath.equals(Iterables.getOnlyElement(allFields).lhs.fieldName)) {
      return makeVectorObjectSelector(
          Collections.emptyList(),
          null /* not used */,
          offset
      );
    }

    AtomicInteger atomicOffset = new AtomicInteger(-1);
    final Supplier<Object> valueProvider;
    if (compressedRawColumnSupplier != null) {
      if (compressedRawColumn == null) {
        compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
      }
      valueProvider = () -> {
        final ByteBuffer valueBuffer = compressedRawColumn.get(atomicOffset.get());
        return STRATEGY.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      };
    } else {
      AtomicIntegerReadableOffset readableAtomicOffset = new AtomicIntegerReadableOffset(atomicOffset);
      final List<Pair<List<NestedPathPart>, ? extends ColumnValueSelector>> fieldSelectors =
          allFields.stream()
                   .map(pair -> Pair.of(
                       pair.rhs,
                       getColumnHolder(
                           pair.lhs.fieldName,
                           pair.lhs.fieldIndex
                       ).getColumn().makeColumnValueSelector(readableAtomicOffset)
                   ))
                   .collect(Collectors.toList());
      valueProvider = () -> {
        List<StructuredDataBuilder.Element> elements = fieldSelectors
            .stream()
            .map(c -> StructuredDataBuilder.Element.of(c.lhs, c.rhs.getObject()))
            .collect(Collectors.toList());
        return new StructuredDataBuilder(elements).build();
      };
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
        atomicOffset.set(offset);
        return valueProvider.get();
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
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    List<NestedField> allFields = getAllNestedFields();
    if (!logicalType.equals(ColumnType.NESTED_DATA)
        && allFields.size() == 1
        && rootFieldPath.equals(Iterables.getOnlyElement(allFields).fieldName)) {
      return makeVectorValueSelector(
          Collections.emptyList(),
          null /* not used */,
          offset
      );
    }
    return super.makeVectorValueSelector(offset);
  }

  @Override
  public int getLength()
  {
    if (compressedRawColumn == null && compressedRawColumnSupplier != null) {
      compressedRawColumn = closer.register(compressedRawColumnSupplier.get());
    }
    return compressedRawColumnSupplier != null ? compressedRawColumn.size() : -1;
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(closer);
  }


  /**
   * Create a selector for a nested path.
   *
   * @param path            the path
   * @param selectorFactory unused
   * @param readableOffset  offset for the selector
   */
  @Override
  public DimensionSelector makeDimensionSelector(
      List<NestedPathPart> path,
      ExtractionFn extractionFn,
      ColumnSelectorFactory selectorFactory,
      ReadableOffset readableOffset
  )
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) getColumnHolder(
          ((NestedField) field).fieldName,
          ((NestedField) field).fieldIndex
      ).getColumn();
      return col.makeDimensionSelector(readableOffset, extractionFn);
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final int elementNumber = arrayField.elementNumber;
      if (elementNumber < 0) {
        throw new IAE(
            "Cannot make array element selector for path [%s], negative array index not supported for this selector",
            path
        );
      }
      ColumnValueSelector<?> arraySelector = getColumnHolder(
          arrayField.nestedField.fieldName,
          arrayField.nestedField.fieldIndex
      ).getColumn().makeColumnValueSelector(readableOffset);
      return new BaseSingleValueDimensionSelector()
      {
        @Nullable
        @Override
        protected String getValue()
        {
          Object o = arraySelector.getObject();
          if (o instanceof Object[]) {
            Object[] array = (Object[]) o;
            if (elementNumber < array.length) {
              Object element = array[elementNumber];
              if (element == null) {
                return null;
              }
              return String.valueOf(element);
            }
          }
          return null;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          arraySelector.inspectRuntimeShape(inspector);
        }
      };

    }
    return DimensionSelector.constant(null);
  }

  /**
   * Create a selector for a nested path.
   *
   * @param path            the path
   * @param selectorFactory unused
   * @param readableOffset  offset for the selector
   */
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      List<NestedPathPart> path,
      ColumnSelectorFactory selectorFactory,
      ReadableOffset readableOffset
  )
  {
    Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      final NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex).getColumn()
                                                                           .makeColumnValueSelector(readableOffset);
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final int elementNumber = arrayField.elementNumber;
      if (elementNumber < 0) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Cannot make array element selector for path [%s], negative array index not supported for this selector",
                                path
                            );
      }
      ColumnValueSelector<?> arraySelector = getColumnHolder(
          arrayField.nestedField.fieldName,
          arrayField.nestedField.fieldIndex
      ).getColumn().makeColumnValueSelector(readableOffset);
      return new ColumnValueSelector<>()
      {
        @Override
        public boolean isNull()
        {
          Object o = getObject();
          return !(o instanceof Number);
        }

        @Override
        public long getLong()
        {
          Object o = getObject();
          return o instanceof Number ? ((Number) o).longValue() : 0L;
        }

        @Override
        public float getFloat()
        {
          Object o = getObject();
          return o instanceof Number ? ((Number) o).floatValue() : 0f;
        }

        @Override
        public double getDouble()
        {
          Object o = getObject();
          return o instanceof Number ? ((Number) o).doubleValue() : 0.0;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          arraySelector.inspectRuntimeShape(inspector);
        }

        @Nullable
        @Override
        public Object getObject()
        {
          Object o = arraySelector.getObject();
          if (o instanceof Object[]) {
            Object[] array = (Object[]) o;
            if (elementNumber < array.length) {
              return array[elementNumber];
            }
          }
          return null;
        }

        @Override
        public Class<?> classOfObject()
        {
          return Object.class;
        }
      };

    }
    return NilColumnValueSelector.instance();
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory selectorFactory,
      ReadableVectorOffset readableOffset
  )
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      NestedField nestedField = (NestedField) field;
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) getColumnHolder(
          nestedField.fieldName,
          nestedField.fieldIndex
      ).getColumn();
      return col.makeSingleValueDimensionVectorSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  /**
   * Create a selector for a nested path.
   *
   * @param path            the path
   * @param selectorFactory unused
   * @param readableOffset  offset for the selector
   */
  @Override
  public VectorObjectSelector makeVectorObjectSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory selectorFactory,
      ReadableVectorOffset readableOffset
  )
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex).getColumn()
                                                                           .makeVectorObjectSelector(readableOffset);
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final int elementNumber = arrayField.elementNumber;
      if (elementNumber < 0) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Cannot make array element selector for path [%s], negative array index not supported for this selector",
                                path
                            );
      }
      VectorObjectSelector arraySelector = getColumnHolder(
          arrayField.nestedField.fieldName,
          arrayField.nestedField.fieldIndex
      ).getColumn().makeVectorObjectSelector(readableOffset);
      return new VectorObjectSelector()
      {
        private final Object[] elements = new Object[arraySelector.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public Object[] getObjectVector()
        {
          if (readableOffset.getId() != id) {
            final Object[] delegate = arraySelector.getObjectVector();
            for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
              Object maybeArray = delegate[i];
              if (maybeArray instanceof Object[]) {
                Object[] anArray = (Object[]) maybeArray;
                if (elementNumber < anArray.length) {
                  final Object element = anArray[elementNumber];
                  elements[i] = element;
                } else {
                  elements[i] = null;
                }
              } else {
                elements[i] = null;
              }
            }
            id = readableOffset.getId();
          }
          return elements;
        }

        @Override
        public int getMaxVectorSize()
        {
          return arraySelector.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return arraySelector.getCurrentVectorSize();
        }
      };
    }

    return NilVectorSelector.create(readableOffset);
  }

  /**
   * Create a selector for a nested path.
   *
   * @param path            the path
   * @param selectorFactory unused
   * @param readableOffset  offset for the selector
   */
  @Override
  public VectorValueSelector makeVectorValueSelector(
      List<NestedPathPart> path,
      VectorColumnSelectorFactory selectorFactory,
      ReadableVectorOffset readableOffset
  )
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex).getColumn()
                                                                           .makeVectorValueSelector(readableOffset);
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final int elementNumber = arrayField.elementNumber;
      if (elementNumber < 0) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Cannot make array element selector for path [%s], negative array index not supported for this selector",
                                path
                            );
      }
      VectorObjectSelector arraySelector = getColumnHolder(
          arrayField.nestedField.fieldName,
          arrayField.nestedField.fieldIndex
      ).getColumn().makeVectorObjectSelector(readableOffset);

      return new VectorValueSelector()
      {
        private final long[] longs = new long[readableOffset.getMaxVectorSize()];
        private final double[] doubles = new double[readableOffset.getMaxVectorSize()];
        private final float[] floats = new float[readableOffset.getMaxVectorSize()];
        private final boolean[] nulls = new boolean[readableOffset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        private void computeNumbers()
        {
          if (readableOffset.getId() != id) {
            final Object[] maybeArrays = arraySelector.getObjectVector();
            for (int i = 0; i < arraySelector.getCurrentVectorSize(); i++) {
              Object maybeArray = maybeArrays[i];
              if (maybeArray instanceof Object[]) {
                Object[] anArray = (Object[]) maybeArray;
                if (elementNumber < anArray.length) {
                  if (anArray[elementNumber] instanceof Number) {
                    Number n = (Number) anArray[elementNumber];
                    longs[i] = n.longValue();
                    doubles[i] = n.doubleValue();
                    floats[i] = n.floatValue();
                    nulls[i] = false;
                  } else {
                    Double d = anArray[elementNumber] instanceof String
                               ? Doubles.tryParse((String) anArray[elementNumber])
                               : null;
                    if (d != null) {
                      longs[i] = d.longValue();
                      doubles[i] = d;
                      floats[i] = d.floatValue();
                      nulls[i] = false;
                    } else {
                      nullElement(i);
                    }
                  }
                } else {
                  nullElement(i);
                }
              } else {
                // not an array?
                nullElement(i);
              }
            }
            id = readableOffset.getId();
          }
        }

        private void nullElement(int i)
        {
          longs[i] = 0L;
          doubles[i] = 0L;
          floats[i] = 0L;
          nulls[i] = true;
        }

        @Override
        public long[] getLongVector()
        {
          if (readableOffset.getId() != id) {
            computeNumbers();
          }
          return longs;
        }

        @Override
        public float[] getFloatVector()
        {
          if (readableOffset.getId() != id) {
            computeNumbers();
          }
          return floats;
        }

        @Override
        public double[] getDoubleVector()
        {
          if (readableOffset.getId() != id) {
            computeNumbers();
          }
          return doubles;
        }

        @Nullable
        @Override
        public boolean[] getNullVector()
        {
          if (readableOffset.getId() != id) {
            computeNumbers();
          }
          return nulls;
        }

        @Override
        public int getMaxVectorSize()
        {
          return arraySelector.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return arraySelector.getCurrentVectorSize();
        }
      };
    }
    return NilVectorSelector.create(readableOffset);
  }


  @Nullable
  @Override
  public Set<ColumnType> getFieldTypes(List<NestedPathPart> path)
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      return FieldTypeInfo.convertToSet(fieldInfo.getTypes(((NestedField) field).fieldIndex).getByteValue());
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final Set<ColumnType> arrayFieldTypes = FieldTypeInfo.convertToSet(fieldInfo.getTypes(arrayField.nestedField.fieldIndex)
                                                                                  .getByteValue());
      final Set<ColumnType> elementTypes = Sets.newHashSetWithExpectedSize(arrayFieldTypes.size());
      for (ColumnType type : arrayFieldTypes) {
        if (type.isArray()) {
          elementTypes.add((ColumnType) type.getElementType());
        } else {
          elementTypes.add(type);
        }
      }
      return elementTypes;
    }
    return null;
  }

  @Nullable
  @Override
  public ColumnType getFieldLogicalType(List<NestedPathPart> path)
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      final Set<ColumnType> fieldTypes = FieldTypeInfo.convertToSet(fieldInfo.getTypes(((NestedField) field).fieldIndex)
                                                                             .getByteValue());
      return ColumnType.leastRestrictiveType(fieldTypes);
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement arrayField = (NestedArrayElement) field;
      final Set<ColumnType> arrayFieldTypes = FieldTypeInfo.convertToSet(fieldInfo.getTypes(arrayField.nestedField.fieldIndex)
                                                                                  .getByteValue());
      ColumnType leastRestrictiveType = null;
      for (ColumnType type : arrayFieldTypes) {
        if (type.isArray()) {
          leastRestrictiveType = ColumnType.leastRestrictiveType(
              leastRestrictiveType,
              (ColumnType) type.getElementType()
          );
        } else {
          leastRestrictiveType = ColumnType.leastRestrictiveType(leastRestrictiveType, type);
        }
      }
      return leastRestrictiveType;

    }
    return null;
  }

  @Nullable
  @Override
  public BaseColumnHolder getColumnHolder(List<NestedPathPart> path)
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      final NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex);
    }
    return null;
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path)
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      final NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex).getIndexSupplier();
    } else if (field instanceof NestedArrayElement) {
      return NoIndexesColumnIndexSupplier.getInstance();
    }
    return null;
  }

  @Override
  public boolean isNumeric(List<NestedPathPart> path)
  {
    final Field field = getNestedFieldOrNestedArrayElementFromPath(path);
    if (field instanceof NestedField) {
      final NestedField nestedField = (NestedField) field;
      return getColumnHolder(nestedField.fieldName, nestedField.fieldIndex).getCapabilities().isNumeric();
    } else if (field instanceof NestedArrayElement) {
      final NestedArrayElement element = (NestedArrayElement) field;
      final TypeSignature<ValueType> elementType = getColumnHolder(
          element.nestedField.fieldName,
          element.nestedField.fieldIndex
      ).getCapabilities().getElementType();
      if (elementType != null) {
        return elementType.isNumeric();
      }
      // if element type is null, the field was not an array, so don't consider it as numeric
      return false;
    }
    // a non-existent field can be considered numeric via a nil selector
    return field == null;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    final Function<CompressedNestedDataComplexColumn, ?> asFn = AS_MAP.get(clazz);
    if (asFn != null) {
      return (T) asFn.apply(this);
    } else {
      return super.as(clazz);
    }
  }

  private BaseColumnHolder getColumnHolder(String field, int fieldIndex)
  {
    return columns.computeIfAbsent(fieldIndex, (f) -> readNestedFieldColumn(field, fieldIndex));
  }

  @Nullable
  private BaseColumnHolder readNestedFieldColumn(String field, int fieldIndex)
  {
    try {
      if (fieldIndex < 0) {
        return null;
      }
      final FieldTypeInfo.TypeSet types = fieldInfo.getTypes(fieldIndex);
      final String fieldFileName = getFieldFileName(columnName, field, fieldIndex);
      final ByteBuffer dataBuffer = fileMapper.mapFile(fieldFileName);
      if (dataBuffer == null) {
        throw new ISE(
            "Can't find field [%s] with name [%s] in [%s] file.",
            field,
            fieldFileName,
            columnName
        );
      }

      ColumnBuilder columnBuilder = new ColumnBuilder().setFileMapper(fileMapper);
      // heh, maybe this should be its own class, or DictionaryEncodedColumnPartSerde could be cooler
      DictionaryEncodedColumnPartSerde.VERSION version = DictionaryEncodedColumnPartSerde.VERSION.fromByte(
          dataBuffer.get()
      );
      // we should check this someday soon, but for now just read it to push the buffer position ahead
      int flags = dataBuffer.getInt();
      if (flags != DictionaryEncodedColumnPartSerde.NO_FLAGS) {
        throw DruidException.defensive(
            "Unrecognized bits set in space reserved for future flags for field column [%s]", field
        );
      }

      final Supplier<FixedIndexed<Integer>> localDictionarySupplier = FixedIndexed.read(
          dataBuffer,
          INT_TYPE_STRATEGY,
          byteOrder,
          Integer.BYTES
      );
      ByteBuffer bb = dataBuffer.asReadOnlyBuffer().order(byteOrder);
      int longsLength = bb.getInt();
      int doublesLength = bb.getInt();
      dataBuffer.position(dataBuffer.position() + Integer.BYTES + Integer.BYTES);
      int pos = dataBuffer.position();
      final Supplier<ColumnarLongs> longs = longsLength > 0 ? CompressedColumnarLongsSupplier.fromByteBuffer(
          dataBuffer,
          byteOrder,
          columnBuilder.getFileMapper()
      ) : () -> null;
      dataBuffer.position(pos + longsLength);
      pos = dataBuffer.position();
      final Supplier<ColumnarDoubles> doubles = doublesLength > 0 ? CompressedColumnarDoublesSuppliers.fromByteBuffer(
          dataBuffer,
          byteOrder,
          columnBuilder.getFileMapper()
      ) : () -> null;
      dataBuffer.position(pos + doublesLength);
      final WritableSupplier<ColumnarInts> ints;
      if (version == DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED) {
        ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(dataBuffer, byteOrder, columnBuilder.getFileMapper());
      } else {
        ints = VSizeColumnarInts.readFromByteBuffer(dataBuffer);
      }

      final ColumnType theType = types.getSingleType();
      columnBuilder.setHasMultipleValues(false)
                   .setType(theType != null
                            ? theType
                            : ColumnType.leastRestrictiveType(FieldTypeInfo.convertToSet(types.getByteValue())));
      final BitmapIndexType indexType;
      if (theType != null) {
        if (theType.getType() == ValueType.LONG) {
          indexType = formatSpec.getLongFieldBitmapIndexType();
        } else if (theType.getType() == ValueType.DOUBLE) {
          indexType = formatSpec.getDoubleFieldBitmapIndexType();
        } else {
          indexType = null;
        }
      } else {
        indexType = null;
      }

      final ImmutableBitmap nullBitmap;
      if (indexType == null || indexType instanceof BitmapIndexType.DictionaryEncodedValueIndex) {
        GenericIndexed<ImmutableBitmap> rBitmaps = BitmapIndexType.DictionaryEncodedValueIndex.read(
            dataBuffer,
            formatSpec.getBitmapEncoding().getObjectStrategy(),
            columnBuilder.getFileMapper()
        );
        final Supplier<FixedIndexed<Integer>> arrayElementDictionarySupplier;
        final GenericIndexed<ImmutableBitmap> arrayElementBitmaps;
        if (dataBuffer.hasRemaining()) {
          arrayElementDictionarySupplier = FixedIndexed.read(
              dataBuffer,
              INT_TYPE_STRATEGY,
              byteOrder,
              Integer.BYTES
          );
          arrayElementBitmaps = GenericIndexed.read(
              dataBuffer,
              formatSpec.getBitmapEncoding().getObjectStrategy(),
              columnBuilder.getFileMapper()
          );
        } else {
          arrayElementDictionarySupplier = null;
          arrayElementBitmaps = null;
        }
        final boolean hasNull = localDictionarySupplier.get().get(0) == 0;
        nullBitmap = hasNull
                     ? rBitmaps.get(0)
                     : formatSpec.getBitmapEncoding().getBitmapFactory().makeEmptyImmutableBitmap();
        columnBuilder.setHasNulls(hasNull)
                     .setIndexSupplier(new NestedFieldColumnIndexSupplier(
                         types,
                         formatSpec.getBitmapEncoding().getBitmapFactory(),
                         columnConfig,
                         rBitmaps,
                         localDictionarySupplier,
                         stringDictionarySupplier,
                         longDictionarySupplier,
                         doubleDictionarySupplier,
                         arrayDictionarySupplier,
                         arrayElementDictionarySupplier,
                         arrayElementBitmaps
                     ), true, false);
      } else if (indexType instanceof BitmapIndexType.NullValueIndex) {
        nullBitmap = BitmapIndexType.NullValueIndex.read(
            dataBuffer,
            formatSpec.getBitmapEncoding().getObjectStrategy()
        );
        columnBuilder.setHasNulls(!nullBitmap.isEmpty()).setNullValueIndexSupplier(nullBitmap);
      } else {
        throw DruidException.defensive("Unsupported BitmapIndexType[%s]", indexType);
      }

      columnBuilder.setDictionaryEncodedColumnSupplier(() -> closer.register(new NestedFieldDictionaryEncodedColumn(
          types,
          longs.get(),
          doubles.get(),
          ints.get(),
          stringDictionarySupplier.get(),
          longDictionarySupplier.get(),
          doubleDictionarySupplier.get(),
          arrayDictionarySupplier != null ? arrayDictionarySupplier.get() : null,
          localDictionarySupplier.get(),
          nullBitmap
      )));
      return columnBuilder.build();
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to read data for [%s]", field);
    }
  }

  private static final class IntTypeStrategy implements TypeStrategy<Integer>
  {
    @Override
    public int estimateSizeBytes(Integer value)
    {
      return Integer.BYTES;
    }

    @Override
    public Integer read(ByteBuffer buffer)
    {
      return buffer.getInt();
    }

    @Override
    public Integer read(ByteBuffer buffer, int offset)
    {
      return buffer.getInt(offset);
    }

    @Override
    public boolean readRetainsBufferReference()
    {
      return false;
    }

    @Override
    public int write(ByteBuffer buffer, Integer value, int maxSizeBytes)
    {
      TypeStrategies.checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.LONG);
      final int sizeBytes = Integer.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putInt(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Object o1, Object o2)
    {
      return Integer.compare(((Number) o1).intValue(), ((Number) o2).intValue());
    }
  }

  private List<NestedField> getAllNestedFields()
  {
    TKeyDictionary fields = fieldsSupplier.get();
    List<NestedField> allFields = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      String field = StringUtils.fromUtf8(fields.get(i));
      allFields.add(new NestedField(field, i));
    }
    return allFields;
  }

  private List<Pair<NestedField, List<NestedPathPart>>> getAllParsedNestedFields()
  {
    TKeyDictionary fields = fieldsSupplier.get();
    List<Pair<NestedField, List<NestedPathPart>>> allFields = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      String field = StringUtils.fromUtf8(fields.get(i));
      allFields.add(Pair.of(new NestedField(field, i), parsePath(field)));
    }
    return allFields;
  }

  /**
   * Returns a representation of a field or array element within a nested object structure, given a path.
   * <p>
   * Returns null if the path does not correspond to any field or array element.
   */
  @Nullable
  private Field getNestedFieldOrNestedArrayElementFromPath(List<NestedPathPart> path)
  {
    TKeyDictionary fields = fieldsSupplier.get();
    List<List<NestedPathPart>> parsed = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      String field = StringUtils.fromUtf8(fields.get(i));
      parsed.add(parsePath(field));
      if (parsed.get(i).equals(path)) {
        return new NestedField(field, i);
      }
    }
    if (!path.isEmpty() && path.get(path.size() - 1) instanceof NestedPathArrayElement) {
      List<NestedPathPart> arrayPath = path.subList(0, path.size() - 1);
      for (int i = 0; i < fields.size(); i++) {
        if (parsed.get(i).equals(arrayPath)) {
          return new NestedArrayElement(
              new NestedField(StringUtils.fromUtf8(fields.get(i)), i),
              ((NestedPathArrayElement) path.get(path.size() - 1)).getIndex()
          );
        }
      }
    }
    return null;
  }

  /**
   * Represents a single target element within a nested object structure.
   */
  interface Field
  {
  }

  /**
   * Represents a field located within a nested object hierarchy, could be scalar or array.
   */
  private static class NestedField implements Field
  {
    private final String fieldName;
    private final int fieldIndex;

    NestedField(String fieldName, int fieldIndex)
    {
      this.fieldName = fieldName;
      this.fieldIndex = fieldIndex;
    }
  }

  /**
   * Represents an element located within an array field inside a nested object hierarchy.
   */
  private static class NestedArrayElement implements Field
  {
    private final NestedField nestedField;
    private final int elementNumber;

    NestedArrayElement(NestedField nestedField, int elementNumber)
    {
      this.nestedField = nestedField;
      this.elementNumber = elementNumber;
    }
  }
}
