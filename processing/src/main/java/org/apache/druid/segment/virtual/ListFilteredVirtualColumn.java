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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdMapping;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.DruidPredicateIndex;
import org.apache.druid.segment.column.LexicographicalRangeIndex;
import org.apache.druid.segment.column.NullValueIndex;
import org.apache.druid.segment.column.SimpleBitmapColumnIndex;
import org.apache.druid.segment.column.SimpleImmutableBitmapIterableIndex;
import org.apache.druid.segment.column.StringValueSetIndex;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

/**
 * {@link VirtualColumn} form of {@link ListFilteredDimensionSpec}, powered by
 * {@link org.apache.druid.query.dimension.ForwardingFilteredDimensionSelector}
 */
public class ListFilteredVirtualColumn implements VirtualColumn
{
  private final String name;
  private final DimensionSpec delegate;
  private final Set<String> values;
  private final boolean allowList;

  @JsonCreator
  public ListFilteredVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("values") Set<String> values,
      @JsonProperty("isAllowList") @Nullable Boolean isAllowList
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.delegate = delegate;
    this.values = values;
    this.allowList = isAllowList == null ? true : isAllowList.booleanValue();
  }


  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public Set<String> getValues()
  {
    return values;
  }

  @JsonProperty("isAllowList")
  public boolean isAllowList()
  {
    return allowList;
  }

  @JsonProperty
  public DimensionSpec getDelegate()
  {
    return delegate;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_LIST_FILTERED)
        .appendString(name)
        .appendCacheable(delegate)
        .appendStringsIgnoringOrder(values)
        .appendBoolean(allowList);
    return builder.build();
  }


  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    if (allowList) {
      return ListFilteredDimensionSpec.filterAllowList(values, factory.makeDimensionSelector(delegate));
    } else {
      return ListFilteredDimensionSpec.filterDenyList(values, factory.makeDimensionSelector(delegate));
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    return makeDimensionSelector(DefaultDimensionSpec.of(columnName), factory);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return new ColumnCapabilitiesImpl().setType(delegate.getOutputType())
                                       .setDictionaryEncoded(true)
                                       .setHasBitmapIndexes(true);
  }

  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return inspector.getColumnCapabilities(delegate.getDimension());
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(delegate.getDimension());
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(String columnName, ColumnSelector columnSelector)
  {
    return new ColumnIndexSupplier()
    {
      @Nullable
      @Override
      public <T> T as(Class<T> clazz)
      {

        final ColumnHolder holder = columnSelector.getColumnHolder(delegate.getDimension());
        if (holder == null) {
          return null;
        }
        // someday maybe we can have a better way to get row count..
        final ColumnHolder time = columnSelector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
        final int numRows = time.getLength();

        ColumnIndexSupplier indexSupplier = holder.getIndexSupplier();
        if (indexSupplier == null) {
          return null;
        }
        DictionaryEncodedStringValueIndex underlyingIndex = indexSupplier.as(
            DictionaryEncodedStringValueIndex.class
        );
        if (underlyingIndex == null) {
          return null;
        }

        final IdMapping idMapping;
        if (allowList) {
          idMapping = ListFilteredDimensionSpec.buildAllowListIdMapping(
              values,
              underlyingIndex.getCardinality(),
              null,
              underlyingIndex::getValue
          );
        } else {
          idMapping = ListFilteredDimensionSpec.buildDenyListIdMapping(
              values,
              underlyingIndex.getCardinality(),
              underlyingIndex::getValue
          );
        }

        if (clazz.equals(NullValueIndex.class)) {
          return (T) new ListFilteredNullValueIndex(underlyingIndex, idMapping, numRows);
        } else if (clazz.equals(StringValueSetIndex.class)) {
          return (T) new ListFilteredStringValueSetIndex(underlyingIndex, idMapping);
        } else if (clazz.equals(DruidPredicateIndex.class)) {
          return (T) new ListFilteredDruidPredicateIndex(underlyingIndex, idMapping);
        } else if (clazz.equals(LexicographicalRangeIndex.class)) {
          return (T) new ListFilteredLexicographicalRangeIndex(underlyingIndex, idMapping);
        } else if (clazz.equals(DictionaryEncodedStringValueIndex.class) || clazz.equals(DictionaryEncodedValueIndex.class)) {
          return (T) new ListFilteredDictionaryEncodedStringValueIndex(underlyingIndex, idMapping);
        }
        return null;
      }
    };
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
    ListFilteredVirtualColumn that = (ListFilteredVirtualColumn) o;
    return allowList == that.allowList && name.equals(that.name) && delegate.equals(that.delegate) && values.equals(
        that.values);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, delegate, values, allowList);
  }

  @Override
  public String toString()
  {
    return "ListFilteredVirtualColumn{" +
           "name='" + name + '\'' +
           ", delegate=" + delegate +
           ", values=" + values +
           ", isAllowList=" + allowList +
           '}';
  }

  private static class BaseListFilteredColumnIndex
  {
    final DictionaryEncodedStringValueIndex delegate;
    final IdMapping idMapping;

    private BaseListFilteredColumnIndex(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping
    )
    {
      this.delegate = delegate;
      this.idMapping = idMapping;
    }

    ImmutableBitmap getBitmap(int idx)
    {
      return delegate.getBitmap(idMapping.getReverseId(idx));
    }

    int getCardinality()
    {
      return idMapping.getValueCardinality();
    }

    int getReverseIndex(@Nullable String value)
    {
      int minIndex = 0;
      int maxIndex = idMapping.getValueCardinality() - 1;
      final Comparator<String> comparator = StringComparators.LEXICOGRAPHIC;
      while (minIndex <= maxIndex) {
        int currIndex = (minIndex + maxIndex) >>> 1;

        String currValue = delegate.getValue(idMapping.getReverseId(currIndex));
        int comparison = comparator.compare(currValue, value);
        if (comparison == 0) {
          return currIndex;
        }

        if (comparison < 0) {
          minIndex = currIndex + 1;
        } else {
          maxIndex = currIndex - 1;
        }
      }

      return -(minIndex + 1);
    }

    Iterable<ImmutableBitmap> getBitmapsInRange(Predicate<String> matcher, int start, int end)
    {
      return () -> new Iterator<ImmutableBitmap>()
      {
        int currIndex = start;
        int found;

        {
          found = findNext();
        }

        private int findNext()
        {
          while (currIndex < end && !matcher.apply(delegate.getValue(idMapping.getReverseId(currIndex)))) {
            currIndex++;
          }

          if (currIndex < end) {
            return currIndex++;
          } else {
            return -1;
          }
        }

        @Override
        public boolean hasNext()
        {
          return found != -1;
        }

        @Override
        public ImmutableBitmap next()
        {
          int cur = found;

          if (cur == -1) {
            throw new NoSuchElementException();
          }

          found = findNext();
          return getBitmap(cur);
        }
      };
    }
  }

  private static class ListFilteredNullValueIndex extends BaseListFilteredColumnIndex implements NullValueIndex
  {
    private final int numRows;

    private ListFilteredNullValueIndex(DictionaryEncodedStringValueIndex delegate, IdMapping idMapping, int numRows)
    {
      super(delegate, idMapping);
      this.numRows = numRows;
    }

    @Override
    public BitmapColumnIndex forNull()
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return 1.0 - Filters.estimateSelectivity(getBitmapIterable().iterator(), totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.complement(
              bitmapResultFactory.unionDimensionValueBitmaps(getBitmapIterable()),
              numRows
          );
        }

        @Override
        protected Iterable<ImmutableBitmap> getBitmapIterable()
        {
          final int start = NullHandling.isNullOrEquivalent(delegate.getValue(idMapping.getReverseId(0))) ? 1 : 0;
          return getBitmapsInRange(v -> true, start, idMapping.getValueCardinality());
        }
      };
    }
  }

  private static class ListFilteredStringValueSetIndex extends BaseListFilteredColumnIndex
      implements StringValueSetIndex
  {

    private ListFilteredStringValueSetIndex(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping
    )
    {
      super(delegate, idMapping);
    }

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          return Math.min(1, (double) getBitmapForValue().size() / totalRows);
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          return bitmapResultFactory.wrapDimensionValue(getBitmapForValue());
        }

        private ImmutableBitmap getBitmapForValue()
        {
          int reverseIndex = getReverseIndex(value);
          if (reverseIndex < 0) {
            return delegate.getBitmap(-1);
          }
          return delegate.getBitmap(idMapping.getReverseId(reverseIndex));
        }
      };
    }

    @Override
    public BitmapColumnIndex forSortedValues(SortedSet<String> values)
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          return () -> new Iterator<ImmutableBitmap>()
          {
            final Iterator<String> iterator = values.iterator();
            int next = -1;

            @Override
            public boolean hasNext()
            {
              if (next < 0) {
                findNext();
              }
              return next >= 0;
            }

            @Override
            public ImmutableBitmap next()
            {
              if (next < 0) {
                findNext();
                if (next < 0) {
                  throw new NoSuchElementException();
                }
              }
              final int swap = next;
              next = -1;
              return getBitmap(swap);
            }

            private void findNext()
            {
              while (next < 0 && iterator.hasNext()) {
                String nextValue = iterator.next();
                next = getReverseIndex(nextValue);
              }
            }
          };
        }
      };
    }
  }

  private static class ListFilteredDruidPredicateIndex extends BaseListFilteredColumnIndex
      implements DruidPredicateIndex
  {

    private ListFilteredDruidPredicateIndex(DictionaryEncodedStringValueIndex delegate, IdMapping idMapping)
    {
      super(delegate, idMapping);
    }

    @Override
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public double estimateSelectivity(int totalRows)
        {
          final int start = 0, end = getCardinality();
          return Filters.estimateSelectivity(
              getBitmapsInRange(matcherFactory.makeStringPredicate(), start, end).iterator(),
              totalRows
          );
        }

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory)
        {
          final int start = 0, end = getCardinality();
          return bitmapResultFactory.unionDimensionValueBitmaps(
              getBitmapsInRange(matcherFactory.makeStringPredicate(), start, end)
          );
        }
      };
    }
  }

  private static class ListFilteredLexicographicalRangeIndex extends BaseListFilteredColumnIndex
      implements LexicographicalRangeIndex
  {

    private ListFilteredLexicographicalRangeIndex(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping
    )
    {
      super(delegate, idMapping);
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return forRange(startValue, startStrict, endValue, endStrict, Predicates.alwaysTrue());
    }

    @Override
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        Predicate<String> matcher
    )
    {
      return new SimpleImmutableBitmapIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          int startIndex, endIndex;
          if (startValue == null) {
            startIndex = 0;
          } else {
            final int found = getReverseIndex(NullHandling.emptyToNullIfNeeded(startValue));
            if (found >= 0) {
              startIndex = startStrict ? found + 1 : found;
            } else {
              startIndex = -(found + 1);
            }
          }

          if (endValue == null) {
            endIndex = idMapping.getValueCardinality();
          } else {
            final int found = getReverseIndex(NullHandling.emptyToNullIfNeeded(endValue));
            if (found >= 0) {
              endIndex = endStrict ? found : found + 1;
            } else {
              endIndex = -(found + 1);
            }
          }

          endIndex = Math.max(startIndex, endIndex);
          final int start = startIndex, end = endIndex;
          return getBitmapsInRange(matcher, start, end);
        }
      };
    }
  }

  private static class ListFilteredDictionaryEncodedStringValueIndex extends BaseListFilteredColumnIndex
      implements DictionaryEncodedStringValueIndex
  {
    private ListFilteredDictionaryEncodedStringValueIndex(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping
    )
    {
      super(delegate, idMapping);
    }

    @Override
    public int getCardinality()
    {
      return idMapping.getValueCardinality();
    }

    @Nullable
    @Override
    public String getValue(int index)
    {
      return delegate.getValue(idMapping.getReverseId(index));
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return delegate.getBitmap(idMapping.getReverseId(idx));
    }
  }
}
