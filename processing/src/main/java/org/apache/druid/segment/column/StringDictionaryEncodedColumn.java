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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.CachingIndexed;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
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

/**
 *
 */
public class StringDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
{
  @Nullable
  private final ColumnarInts column;
  @Nullable
  private final ColumnarMultiInts multiValueColumn;
  private final CachingIndexed<String> cachedDictionary;
  private final Indexed<ByteBuffer> dictionaryUtf8;

  public StringDictionaryEncodedColumn(
      @Nullable ColumnarInts singleValueColumn,
      @Nullable ColumnarMultiInts multiValueColumn,
      CachingIndexed<String> dictionary,
      Indexed<ByteBuffer> dictionaryUtf8
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.cachedDictionary = dictionary;
    this.dictionaryUtf8 = dictionaryUtf8;
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
    return multiValueColumn.get(rowNum);
  }

  @Override
  @Nullable
  public String lookupName(int id)
  {
    return cachedDictionary.get(id);
  }


  /**
   * Returns the value for a particular dictionary id as UTF-8 bytes.
   *
   * The returned buffer is in big-endian order. It is not reused, so callers may modify the position, limit, byte
   * order, etc of the buffer.
   *
   * The returned buffer points to the original data, so callers must take care not to use it outside the valid
   * lifetime of this column.
   *
   * @param id id to lookup the dictionary value for
   *
   * @return dictionary value for the given id, or null if the value is itself null
   */
  @Nullable
  public ByteBuffer lookupNameUtf8(int id)
  {
    return dictionaryUtf8.get(id);
  }

  @Override
  public int lookupId(String name)
  {
    return cachedDictionary.indexOf(name);
  }

  @Override
  public int getCardinality()
  {
    return cachedDictionary.size();
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
        final String value = StringDictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ? value : extractionFn.apply(value);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return StringDictionaryEncodedColumn.this.lookupNameUtf8(id);
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
        return StringDictionaryEncodedColumn.this.lookupId(name);
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
        public ValueMatcher makeValueMatcher(Predicate<String> predicate)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
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
            final int valueId = lookupId(value);
            if (valueId >= 0) {
              return new ValueMatcher()
              {
                @Override
                public boolean matches()
                {
                  return getRowValue() == valueId;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  inspector.visit("column", StringDictionaryEncodedColumn.this);
                }
              };
            } else {
              return BooleanValueMatcher.of(false);
            }
          } else {
            // Employ caching BitSet optimization
            return makeValueMatcher(Predicates.equalTo(value));
          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          final BitSet checkedIds = new BitSet(getCardinality());
          final BitSet matchingIds = new BitSet(getCardinality());

          // Lazy matcher; only check an id if matches() is called.
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              final int id = getRowValue();

              if (checkedIds.get(id)) {
                return matchingIds.get(id);
              } else {
                final boolean matches = predicate.apply(lookupName(id));
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
              inspector.visit("column", StringDictionaryEncodedColumn.this);
            }
          };
        }

        @Override
        public Object getObject()
        {
          return lookupName(getRowValue());
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
    class QueryableSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector, IdLookup
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

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
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return StringDictionaryEncodedColumn.this.lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return StringDictionaryEncodedColumn.this.lookupNameUtf8(id);
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
      public int lookupId(@Nullable final String name)
      {
        return StringDictionaryEncodedColumn.this.lookupId(name);
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

    return new QueryableSingleValueDimensionVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    class QueryableMultiValueDimensionVectorSelector implements MultiValueDimensionVectorSelector, IdLookup
    {
      private final IndexedInts[] vector = new IndexedInts[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

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
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return StringDictionaryEncodedColumn.this.lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return StringDictionaryEncodedColumn.this.lookupNameUtf8(id);
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
      public int lookupId(@Nullable final String name)
      {
        return StringDictionaryEncodedColumn.this.lookupId(name);
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

    return new QueryableMultiValueDimensionVectorSelector();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (!hasMultipleValues()) {
      class DictionaryEncodedStringSingleValueVectorObjectSelector implements VectorObjectSelector
      {
        private final int[] vector = new int[offset.getMaxVectorSize()];
        private final Object[] strings = new Object[offset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

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
      }

      return new DictionaryEncodedStringSingleValueVectorObjectSelector();
    } else {
      throw new UnsupportedOperationException("Multivalue string object selector not implemented yet");
    }
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(cachedDictionary, column, multiValueColumn);
  }
}
