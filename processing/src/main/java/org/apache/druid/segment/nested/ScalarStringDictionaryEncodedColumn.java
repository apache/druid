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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.Indexed;
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
 * {@link NestedCommonFormatColumn} specialization for {@link ColumnType#STRING} with a generic buffer based utf8
 * dictionary. This is used when not using the more specific
 * {@link org.apache.druid.segment.column.StringFrontCodedDictionaryEncodedColumn}, and only supports single value
 * strings.
 */
public class ScalarStringDictionaryEncodedColumn<TIndexed extends Indexed<ByteBuffer>>
    implements DictionaryEncodedColumn<String>, NestedCommonFormatColumn
{
  private final ColumnarInts column;
  private final TIndexed utf8Dictionary;

  public ScalarStringDictionaryEncodedColumn(
      ColumnarInts singleValueColumn,
      TIndexed utf8Dictionary
  )
  {
    this.column = singleValueColumn;
    this.utf8Dictionary = utf8Dictionary;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException("Column is not multi-valued");
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
    class SingleValueQueryableDimensionSelector extends AbstractDimensionSelector
        implements SingleValueHistoricalDimensionSelector, IdLookup, HistoricalDimensionSelector
    {
      private final SingleIndexedInt row = new SingleIndexedInt();

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
        final String value = ScalarStringDictionaryEncodedColumn.this.lookupName(id);
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
        return ScalarStringDictionaryEncodedColumn.this.lookupId(name);
      }

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
                inspector.visit("column", ScalarStringDictionaryEncodedColumn.this);
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
            inspector.visit("column", ScalarStringDictionaryEncodedColumn.this);
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
        return ScalarStringDictionaryEncodedColumn.this.lookupName(id);
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
        return ScalarStringDictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new StringVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException("Column not multi-valued");
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
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
        return ScalarStringDictionaryEncodedColumn.this.lookupName(id);
      }
    }
    return new StringVectorSelector();
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(column);
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
}
