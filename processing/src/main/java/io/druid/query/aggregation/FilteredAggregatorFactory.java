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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class FilteredAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x9;

  private final AggregatorFactory delegate;
  private final DimFilter filter;

  public FilteredAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("filter") DimFilter filter
  )
  {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(filter);

    this.delegate = delegate;
    this.filter = filter;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    final ValueMatcherFactory valueMatcherFactory = new FilteredAggregatorValueMatcherFactory(columnSelectorFactory);
    final ValueMatcher valueMatcher = Filters.toFilter(filter).makeMatcher(valueMatcherFactory);
    return new FilteredAggregator(
        valueMatcher,
        delegate.factorize(columnSelectorFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    final ValueMatcherFactory valueMatcherFactory = new FilteredAggregatorValueMatcherFactory(columnSelectorFactory);
    final ValueMatcher valueMatcher = Filters.toFilter(filter).makeMatcher(valueMatcherFactory);
    return new FilteredBufferAggregator(
        valueMatcher,
        delegate.factorizeBuffered(columnSelectorFactory)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegate.combine(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return delegate.finalizeComputation(object);
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] filterCacheKey = filter.getCacheKey();
    byte[] aggregatorCacheKey = delegate.getCacheKey();
    return ByteBuffer.allocate(1 + filterCacheKey.length + aggregatorCacheKey.length)
                     .put(CACHE_TYPE_ID)
                     .put(filterCacheKey)
                     .put(aggregatorCacheKey)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize();
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return delegate.getAggregatorStartValue();
  }

  @JsonProperty
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
  }

  @Override
  public String toString()
  {
    return "FilteredAggregatorFactory{" +
           ", delegate=" + delegate +
           ", filter=" + filter +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FilteredAggregatorFactory that = (FilteredAggregatorFactory) o;

    if (delegate != null ? !delegate.equals(that.delegate) : that.delegate != null) {
      return false;
    }
    if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = delegate != null ? delegate.hashCode() : 0;
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    return result;
  }

  private static class FilteredAggregatorValueMatcherFactory implements ValueMatcherFactory
  {
    private final ColumnSelectorFactory columnSelectorFactory;

    public FilteredAggregatorValueMatcherFactory(ColumnSelectorFactory columnSelectorFactory)
    {
      this.columnSelectorFactory = columnSelectorFactory;
    }

    @Override
    public ValueMatcher makeValueMatcher(final String dimension, final Comparable value)
    {
      if (getTypeForDimension(dimension) == ValueType.LONG) {
        return Filters.getLongValueMatcher(
            columnSelectorFactory.makeLongColumnSelector(dimension),
            value
        );
      }

      final DimensionSelector selector = columnSelectorFactory.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      // Compare "value" as a String.
      final String valueString = value == null ? null : Strings.emptyToNull(value.toString());

      // Missing columns match a null or empty string value, and don't match anything else.
      if (selector == null) {
        return new BooleanValueMatcher(valueString == null);
      }

      final int cardinality = selector.getValueCardinality();

      if (cardinality >= 0) {
        // Dictionary-encoded dimension. Compare by id instead of by value to save time.
        final int valueId = selector.lookupId(valueString);

        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              // null should match empty rows in multi-value columns
              return valueString == null;
            } else {
              for (int i = 0; i < size; ++i) {
                if (row.get(i) == valueId) {
                  return true;
                }
              }
              return false;
            }
          }
        };
      } else {
        // Not dictionary-encoded. Skip the optimization.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              // null should match empty rows in multi-value columns
              return valueString == null;
            } else {
              for (int i = 0; i < size; ++i) {
                if (Objects.equals(selector.lookupName(row.get(i)), valueString)) {
                  return true;
                }
              }
              return false;
            }
          }
        };
      }
    }

    public ValueMatcher makeValueMatcher(final String dimension, final DruidPredicateFactory predicateFactory)
    {
      ValueType type = getTypeForDimension(dimension);
      switch (type) {
        case LONG:
          return makeLongValueMatcher(dimension, predicateFactory.makeLongPredicate());
        case STRING:
          return makeStringValueMatcher(dimension, predicateFactory.makeStringPredicate());
        default:
          return new BooleanValueMatcher(predicateFactory.makeStringPredicate().apply(null));
      }
    }

    public ValueMatcher makeStringValueMatcher(final String dimension, final Predicate<String> predicate)
    {
      final DimensionSelector selector = columnSelectorFactory.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      final boolean doesMatchNull = predicate.apply(null);

      if (selector == null) {
        return new BooleanValueMatcher(doesMatchNull);
      }

      final int cardinality = selector.getValueCardinality();

      if (cardinality >= 0) {
        // Dictionary-encoded dimension. Check every value; build a bitset of matching ids.
        final BitSet valueIds = new BitSet(cardinality);
        for (int i = 0; i < cardinality; i++) {
          if (predicate.apply(selector.lookupName(i))) {
            valueIds.set(i);
          }
        }

        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              // null should match empty rows in multi-value columns
              return doesMatchNull;
            } else {
              for (int i = 0; i < size; ++i) {
                if (valueIds.get(row.get(i))) {
                  return true;
                }
              }
              return false;
            }
          }
        };
      } else {
        // Not dictionary-encoded. Skip the optimization.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final IndexedInts row = selector.getRow();
            final int size = row.size();
            if (size == 0) {
              // null should match empty rows in multi-value columns
              return doesMatchNull;
            } else {
              for (int i = 0; i < size; ++i) {
                if (predicate.apply(selector.lookupName(row.get(i)))) {
                  return true;
                }
              }
              return false;
            }
          }
        };
      }
    }

    private ValueMatcher makeLongValueMatcher(String dimension, DruidLongPredicate predicate)
    {
      return Filters.getLongPredicateMatcher(
          columnSelectorFactory.makeLongColumnSelector(dimension),
          predicate
      );
    }

    private ValueType getTypeForDimension(String dimension)
    {
      // FilteredAggregatorFactory is sometimes created from a ColumnSelectorFactory that
      // has no knowledge of column capabilities/types.
      // Default to LONG for __time, STRING for everything else.
      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
        return ValueType.LONG;
      }
      ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimension);
      return capabilities == null ? ValueType.STRING : capabilities.getType();
    }
  }
}
