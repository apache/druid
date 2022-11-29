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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * {@link DictionaryEncodedColumn<String>} for a column which uses a {@link FrontCodedIndexed} to store its value
 * dictionary, which 'delta encodes' strings (instead of {@link org.apache.druid.segment.data.GenericIndexed} like
 * {@link StringDictionaryEncodedColumn}).
 *
 * This class is otherwise nearly identical to {@link StringDictionaryEncodedColumn} other than the dictionary
 * difference.
 */
public class StringFrontCodedDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
{
  @Nullable
  private final ColumnarInts column;
  @Nullable
  private final ColumnarMultiInts multiValueColumn;
  private final FrontCodedIndexed utf8Dictionary;

  public StringFrontCodedDictionaryEncodedColumn(
      @Nullable ColumnarInts singleValueColumn,
      @Nullable ColumnarMultiInts multiValueColumn,
      FrontCodedIndexed utf8Dictionary
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
        final String value = StringFrontCodedDictionaryEncodedColumn.this.lookupName(id);
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
        return StringFrontCodedDictionaryEncodedColumn.this.lookupId(name);
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
            final int valueId = super.lookupId(value);
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
                  inspector.visit("column", StringFrontCodedDictionaryEncodedColumn.this);
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
              inspector.visit("column", StringFrontCodedDictionaryEncodedColumn.this);
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
    final class StringVectorSelector extends StringDictionaryEncodedColumn.StringSingleValueDimensionVectorSelector
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
        return StringFrontCodedDictionaryEncodedColumn.this.lookupName(id);
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
        return StringFrontCodedDictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new StringVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(final ReadableVectorOffset offset)
  {
    final class MultiStringVectorSelector extends StringDictionaryEncodedColumn.StringMultiValueDimensionVectorSelector
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
        return StringFrontCodedDictionaryEncodedColumn.this.lookupName(id);
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
        return StringFrontCodedDictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new MultiStringVectorSelector();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    if (!hasMultipleValues()) {
      final class StringVectorSelector extends StringDictionaryEncodedColumn.StringVectorObjectSelector
      {
        public StringVectorSelector()
        {
          super(column, offset);
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return StringFrontCodedDictionaryEncodedColumn.this.lookupName(id);
        }
      }
      return new StringVectorSelector();
    } else {
      final class MultiStringVectorSelector extends StringDictionaryEncodedColumn.MultiValueStringVectorObjectSelector
      {
        public MultiStringVectorSelector()
        {
          super(multiValueColumn, offset);
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return StringFrontCodedDictionaryEncodedColumn.this.lookupName(id);
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
}
