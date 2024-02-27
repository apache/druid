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

package org.apache.druid.segment.column;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * {@link DictionaryEncodedColumn<String>} for a column which has a {@link ByteBuffer} based UTF-8 dictionary.
 * <p>
 * <p>
 * Implements {@link NestedCommonFormatColumn} so it can be used as a reader for single value string specializations
 * of {@link org.apache.druid.segment.AutoTypeColumnIndexer}.
 */
public class StringUtf8DictionaryEncodedColumn implements DictionaryEncodedColumn<String>, NestedCommonFormatColumn
{
  @Nullable
  private final ColumnarInts column;
  @Nullable
  private final ColumnarMultiInts multiValueColumn;
  private final Indexed<ByteBuffer> utf8Dictionary;

  public StringUtf8DictionaryEncodedColumn(
      @Nullable ColumnarInts singleValueColumn,
      @Nullable ColumnarMultiInts multiValueColumn,
      Indexed<ByteBuffer> utf8Dictionary
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.utf8Dictionary = utf8Dictionary;
  }

  @Override
  public int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return column == null;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    if (!hasMultipleValues()) {
      throw new UnsupportedOperationException("Column is not multi-valued");
    }
    return multiValueColumn.get(rowNum);
  }

  @Override
  @Nullable
  public String lookupName(int id)
  {
    final ByteBuffer buffer = utf8Dictionary.get(id);
    if (buffer == null) {
      return null;
    }
    return StringUtils.fromUtf8(buffer);
  }

  @Override
  public int lookupId(String name)
  {
    return utf8Dictionary.indexOf(StringUtils.toUtf8ByteBuffer(name));
  }

  @Override
  public int getCardinality()
  {
    return utf8Dictionary.size();
  }

  @Override
  public HistoricalDimensionSelector makeDimensionSelector(
      final ReadableOffset offset,
      @Nullable final ExtractionFn extractionFn
  )
  {
    abstract class QueryableDimensionSelector extends AbstractDimensionSelector
        implements HistoricalDimensionSelector, IdLookup
    {
      @Override
      public int getValueCardinality()
      {
        /*
         This is technically wrong if
         extractionFn != null && (extractionFn.getExtractionType() != ExtractionFn.ExtractionType.ONE_TO_ONE ||
                                    !extractionFn.preservesOrdering())
         However current behavior allows some GroupBy-V1 queries to work that wouldn't work otherwise and doesn't
         cause any problems due to special handling of extractionFn everywhere.
         See https://github.com/apache/druid/pull/8433
         */
        return getCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        final String value = StringUtf8DictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ? value : extractionFn.apply(value);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return utf8Dictionary.get(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return true;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return extractionFn == null ? this : null;
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn != null) {
          throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
        }
        return StringUtf8DictionaryEncodedColumn.this.lookupId(name);
      }
    }

    if (hasMultipleValues()) {
      class MultiValueDimensionSelector extends QueryableDimensionSelector
      {
        @Override
        public IndexedInts getRow()
        {
          return multiValueColumn.get(offset.getOffset());
        }

        @Override
        public IndexedInts getRow(int offset)
        {
          return multiValueColumn.get(offset);
        }

        @Override
        public ValueMatcher makeValueMatcher(@Nullable String value)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
        }

        @Override
        public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return defaultGetObject();
        }

        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("multiValueColumn", multiValueColumn);
          inspector.visit("offset", offset);
          inspector.visit("extractionFn", extractionFn);
        }
      }
      return new MultiValueDimensionSelector();
    } else {
      class SingleValueQueryableDimensionSelector extends QueryableDimensionSelector
          implements SingleValueHistoricalDimensionSelector
      {
        private final SingleIndexedInt row = new SingleIndexedInt();

        @Override
        public IndexedInts getRow()
        {
          row.setValue(getRowValue());
          return row;
        }

        public int getRowValue()
        {
          return column.get(offset.getOffset());
        }

        @Override
        public IndexedInts getRow(int offset)
        {
          row.setValue(getRowValue(offset));
          return row;
        }

        @Override
        public int getRowValue(int offset)
        {
          return column.get(offset);
        }

        @Override
        public ValueMatcher makeValueMatcher(final @Nullable String value)
        {
          if (extractionFn == null) {
            final int valueId = super.lookupId(value);
            final int nullId = super.lookupId(null);
            if (valueId >= 0) {
              return new ValueMatcher()
              {
                @Override
                public boolean matches(boolean includeUnknown)
                {
                  final int rowId = getRowValue();
                  return (includeUnknown && rowId == nullId) || rowId == valueId;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  inspector.visit("column", StringUtf8DictionaryEncodedColumn.this);
                }
              };
            } else {
              // no nulls, and value isn't in column, we can safely optimize to 'allFalse'
              if (nullId < 0) {
                return ValueMatchers.allFalse();
              }
              return new ValueMatcher()
              {
                @Override
                public boolean matches(boolean includeUnknown)
                {
                  if (includeUnknown && getRowValue() == 0) {
                    return true;
                  }
                  return false;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  inspector.visit("column", StringUtf8DictionaryEncodedColumn.this);
                }
              };
            }
          } else {
            // Employ caching BitSet optimization
            return makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(value));
          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
        {
          final BitSet checkedIds = new BitSet(getCardinality());
          final BitSet matchingIds = new BitSet(getCardinality());
          final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();

          // Lazy matcher; only check an id if matches() is called.
          return new ValueMatcher()
          {
            @Override
            public boolean matches(boolean includeUnknown)
            {
              final int id = getRowValue();

              if (checkedIds.get(id)) {
                return matchingIds.get(id);
              } else {
                final String rowValue = lookupName(id);
                final boolean matches = predicate.apply(rowValue).matches(includeUnknown);
                checkedIds.set(id);
                if (matches) {
                  matchingIds.set(id);
                }
                return matches;
              }
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("column", StringUtf8DictionaryEncodedColumn.this);
            }
          };
        }

        @Override
        public Object getObject()
        {
          return super.lookupName(getRowValue());
        }

        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("column", column);
          inspector.visit("offset", offset);
          inspector.visit("extractionFn", extractionFn);
        }
      }
      return new SingleValueQueryableDimensionSelector();
    }
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    final class StringVectorSelector extends StringSingleValueDimensionVectorSelector
    {
      public StringVectorSelector()
      {
        super(column, offset);
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return StringUtf8DictionaryEncodedColumn.this.lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return utf8Dictionary.get(id);
      }

      @Override
      public int lookupId(@Nullable String name)
      {
        return StringUtf8DictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new StringVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    final class MultiStringVectorSelector extends StringMultiValueDimensionVectorSelector
    {
      public MultiStringVectorSelector()
      {
        super(multiValueColumn, offset);
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return StringUtf8DictionaryEncodedColumn.this.lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return utf8Dictionary.get(id);
      }


      @Override
      public int lookupId(@Nullable String name)
      {
        return StringUtf8DictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new MultiStringVectorSelector();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (!hasMultipleValues()) {
      final class StringVectorSelector extends StringVectorObjectSelector
      {
        public StringVectorSelector()
        {
          super(column, offset);
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return StringUtf8DictionaryEncodedColumn.this.lookupName(id);
        }
      }
      return new StringVectorSelector();
    } else {
      final class MultiStringVectorSelector extends MultiValueStringVectorObjectSelector
      {
        public MultiStringVectorSelector()
        {
          super(multiValueColumn, offset);
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return StringUtf8DictionaryEncodedColumn.this.lookupName(id);
        }
      }
      return new MultiStringVectorSelector();
    }
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(column, multiValueColumn);
  }

  @Override
  public ColumnType getLogicalType()
  {
    return ColumnType.STRING;
  }

  @Override
  public Indexed<String> getStringDictionary()
  {
    return new StringEncodingStrategies.Utf8ToStringIndexed(utf8Dictionary);
  }



  /**
   * Base type for a {@link SingleValueDimensionVectorSelector} for a dictionary encoded {@link ColumnType#STRING}
   * built around a {@link ColumnarInts}. Dictionary not included - BYO dictionary lookup methods.
   *
   * Assumes that all implementations return true for {@link #supportsLookupNameUtf8()}.
   */
  public abstract static class StringSingleValueDimensionVectorSelector
          implements SingleValueDimensionVectorSelector, IdLookup
  {
    private final ColumnarInts column;
    private final ReadableVectorOffset offset;
    private final int[] vector;
    private int id = ReadableVectorInspector.NULL_ID;

    public StringSingleValueDimensionVectorSelector(
            ColumnarInts column,
            ReadableVectorOffset offset
    )
    {
      this.column = column;
      this.offset = offset;
      this.vector = new int[offset.getMaxVectorSize()];
    }

    @Override
    public int[] getRowVector()
    {
      if (id == offset.getId()) {
        return vector;
      }

      if (offset.isContiguous()) {
        column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
      } else {
        column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
      }

      id = offset.getId();
      return vector;
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return true;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return this;
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
  }

  /**
   * Base type for a {@link MultiValueDimensionVectorSelector} for a dictionary encoded {@link ColumnType#STRING}
   * built around a {@link ColumnarMultiInts}. Dictionary not included - BYO dictionary lookup methods.
   *
   * Assumes that all implementations return true for {@link #supportsLookupNameUtf8()}.
   */
  public abstract static class StringMultiValueDimensionVectorSelector
          implements MultiValueDimensionVectorSelector, IdLookup
  {
    private final ColumnarMultiInts multiValueColumn;
    private final ReadableVectorOffset offset;

    private final IndexedInts[] vector;
    private int id = ReadableVectorInspector.NULL_ID;

    public StringMultiValueDimensionVectorSelector(
            ColumnarMultiInts multiValueColumn,
            ReadableVectorOffset offset
    )
    {
      this.multiValueColumn = multiValueColumn;
      this.offset = offset;
      this.vector = new IndexedInts[offset.getMaxVectorSize()];
    }

    @Override
    public IndexedInts[] getRowVector()
    {
      if (id == offset.getId()) {
        return vector;
      }

      if (offset.isContiguous()) {
        final int currentOffset = offset.getStartOffset();
        final int numRows = offset.getCurrentVectorSize();

        for (int i = 0; i < numRows; i++) {
          // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
          vector[i] = multiValueColumn.getUnshared(i + currentOffset);
        }
      } else {
        final int[] offsets = offset.getOffsets();
        final int numRows = offset.getCurrentVectorSize();

        for (int i = 0; i < numRows; i++) {
          // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
          vector[i] = multiValueColumn.getUnshared(offsets[i]);
        }
      }

      id = offset.getId();
      return vector;
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return true;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return true;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return this;
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
  }

  /**
   * Base type for a {@link VectorObjectSelector} for a dictionary encoded {@link ColumnType#STRING}
   * built around a {@link ColumnarInts}. Dictionary not included - BYO dictionary lookup methods.
   */
  public abstract static class StringVectorObjectSelector implements VectorObjectSelector
  {
    private final ColumnarInts column;
    private final ReadableVectorOffset offset;

    private final int[] vector;
    private final Object[] strings;
    private int id = ReadableVectorInspector.NULL_ID;

    public StringVectorObjectSelector(
            ColumnarInts column,
            ReadableVectorOffset offset
    )
    {
      this.column = column;
      this.offset = offset;
      this.vector = new int[offset.getMaxVectorSize()];
      this.strings = new Object[offset.getMaxVectorSize()];
    }

    @Override
    public Object[] getObjectVector()
    {
      if (id == offset.getId()) {
        return strings;
      }

      if (offset.isContiguous()) {
        column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
      } else {
        column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        strings[i] = lookupName(vector[i]);
      }
      id = offset.getId();

      return strings;
    }

    @Override
    public int getMaxVectorSize()
    {
      return offset.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }

    @Nullable
    public abstract String lookupName(int id);
  }

  /**
   * Base type for a {@link VectorObjectSelector} for a dictionary encoded {@link ColumnType#STRING}
   * built around a {@link ColumnarMultiInts}. Dictionary not included - BYO dictionary lookup methods.
   */
  public abstract static class MultiValueStringVectorObjectSelector implements VectorObjectSelector
  {
    private final ColumnarMultiInts multiValueColumn;
    private final ReadableVectorOffset offset;

    private final IndexedInts[] vector;
    private final Object[] strings;
    private int id = ReadableVectorInspector.NULL_ID;

    public MultiValueStringVectorObjectSelector(
            ColumnarMultiInts multiValueColumn,
            ReadableVectorOffset offset
    )
    {
      this.multiValueColumn = multiValueColumn;
      this.offset = offset;
      this.vector = new IndexedInts[offset.getMaxVectorSize()];
      this.strings = new Object[offset.getMaxVectorSize()];
    }

    @Nullable
    public abstract String lookupName(int id);

    @Override
    public Object[] getObjectVector()
    {
      if (id == offset.getId()) {
        return strings;
      }

      if (offset.isContiguous()) {
        final int currentOffset = offset.getStartOffset();
        final int numRows = offset.getCurrentVectorSize();

        for (int i = 0; i < numRows; i++) {
          // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
          vector[i] = multiValueColumn.getUnshared(i + currentOffset);
        }
      } else {
        final int[] offsets = offset.getOffsets();
        final int numRows = offset.getCurrentVectorSize();

        for (int i = 0; i < numRows; i++) {
          // Must use getUnshared, otherwise all elements in the vector could be the same shared object.
          vector[i] = multiValueColumn.getUnshared(offsets[i]);
        }
      }

      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        IndexedInts ithRow = vector[i];
        final int size = ithRow.size();
        if (size == 0) {
          strings[i] = null;
        } else if (size == 1) {
          strings[i] = lookupName(ithRow.get(0));
        } else {
          List<String> row = Lists.newArrayListWithCapacity(size);
          for (int j = 0; j < size; j++) {
            row.add(lookupName(ithRow.get(j)));
          }
          strings[i] = row;
        }
      }

      id = offset.getId();
      return strings;
    }

    @Override
    public int getMaxVectorSize()
    {
      return offset.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return offset.getCurrentVectorSize();
    }
  }
}
