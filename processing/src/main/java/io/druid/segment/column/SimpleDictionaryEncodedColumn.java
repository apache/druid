/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.IdLookup;
import io.druid.segment.data.CachingIndexed;
import io.druid.segment.data.ColumnarInts;
import io.druid.segment.data.ColumnarMultiInts;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ReadableOffset;
import io.druid.segment.data.SingleIndexedInt;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.historical.HistoricalDimensionSelector;
import io.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.BitSet;

/**
*/
public class SimpleDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
{
  private final ColumnarInts column;
  private final ColumnarMultiInts multiValueColumn;
  private final CachingIndexed<String> cachedLookups;

  public SimpleDictionaryEncodedColumn(
      ColumnarInts singleValueColumn,
      ColumnarMultiInts multiValueColumn,
      CachingIndexed<String> cachedLookups
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.cachedLookups = cachedLookups;
  }

  @Override
  public Class<String> getClazz()
  {
    return String.class;
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
    return cachedLookups.get(id);
  }

  @Override
  public int lookupId(String name)
  {
    return cachedLookups.indexOf(name);
  }

  @Override
  public int getCardinality()
  {
    return cachedLookups.size();
  }

  @Override
  public HistoricalDimensionSelector makeDimensionSelector(
      final ReadableOffset offset,
      @Nullable final ExtractionFn extractionFn
  )
  {
    abstract class QueryableDimensionSelector implements HistoricalDimensionSelector, IdLookup
    {
      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        final String value = SimpleDictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ?
               value :
               extractionFn.apply(value);
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
        return SimpleDictionaryEncodedColumn.this.lookupId(name);
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
        public ValueMatcher makeValueMatcher(String value)
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
        public ValueMatcher makeValueMatcher(final String value)
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
                  inspector.visit("column", SimpleDictionaryEncodedColumn.this);
                }
              };
            } else {
              return BooleanValueMatcher.of(false);
            }
          } else {
            // Employ precomputed BitSet optimization
            return makeValueMatcher(Predicates.equalTo(value));
          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          final BitSet predicateMatchingValueIds = DimensionSelectorUtils.makePredicateMatchingSet(
              this,
              predicate
          );
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              return predicateMatchingValueIds.get(getRowValue());
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("column", SimpleDictionaryEncodedColumn.this);
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
  public void close() throws IOException
  {
    CloseQuietly.close(cachedLookups);

    if (column != null) {
      column.close();
    }
    if (multiValueColumn != null) {
      multiValueColumn.close();
    }
  }
}
