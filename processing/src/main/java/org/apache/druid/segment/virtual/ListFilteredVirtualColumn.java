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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdMapping;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleBitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapDelegatingIterableIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DictionaryEncodedValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;

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
      return ListFilteredDimensionSpec.filterAllowList(values, factory.makeDimensionSelector(delegate), delegate.getExtractionFn() != null);
    } else {
      return ListFilteredDimensionSpec.filterDenyList(values, factory.makeDimensionSelector(delegate), delegate.getExtractionFn() != null);
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

  @Nullable
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
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector indexSelector
  )
  {
    if (delegate.getExtractionFn() != null) {
      return NoIndexesColumnIndexSupplier.getInstance();
    }
    return new ColumnIndexSupplier()
    {
      @Nullable
      @Override
      public <T> T as(Class<T> clazz)
      {
        ColumnIndexSupplier indexSupplier = indexSelector.getIndexSupplier(delegate.getDimension());
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

        // someday maybe we can have a better way to get row count..
        final ColumnHolder time = indexSelector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
        final int numRows = time.getLength();
        final Supplier<ImmutableBitmap> nullValueBitmapSupplier = Suppliers.memoize(
            () -> underlyingIndex.getBitmapFactory().complement(
                underlyingIndex.getBitmapFactory().union(getNonNullBitmaps(underlyingIndex, idMapping)),
                numRows
            )
        );

        if (clazz.equals(NullValueIndex.class)) {
          return (T) new ListFilteredNullValueIndex(nullValueBitmapSupplier);
        } else if (clazz.equals(StringValueSetIndexes.class)) {
          return (T) new ListFilteredStringValueSetIndexes(underlyingIndex, idMapping, nullValueBitmapSupplier);
        } else if (clazz.equals(DruidPredicateIndexes.class)) {
          return (T) new ListFilteredDruidPredicateIndexes(underlyingIndex, idMapping, nullValueBitmapSupplier);
        } else if (clazz.equals(LexicographicalRangeIndexes.class)) {
          return (T) new ListFilteredLexicographicalRangeIndexes(underlyingIndex, idMapping, nullValueBitmapSupplier);
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
    return allowList == that.allowList &&
           name.equals(that.name) &&
           delegate.equals(that.delegate) &&
           values.equals(that.values);
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

  private static Iterable<ImmutableBitmap> getNonNullBitmaps(
      DictionaryEncodedStringValueIndex delegateIndex,
      IdMapping idMapping
  )
  {
    final int start = NullHandling.isNullOrEquivalent(delegateIndex.getValue(idMapping.getReverseId(0))) ? 1 : 0;
    return getBitmapsInRange(
        delegateIndex,
        idMapping,
        DruidObjectPredicate.alwaysTrue(),
        start,
        idMapping.getValueCardinality(),
        false
    );
  }

  private static Iterable<ImmutableBitmap> getBitmapsInRange(
      DictionaryEncodedStringValueIndex delegate,
      IdMapping idMapping,
      DruidObjectPredicate<String> matcher,
      int start,
      int end,
      boolean includeUnknown
  )
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
        while (currIndex < end && !matcher.apply(delegate.getValue(idMapping.getReverseId(currIndex))).matches(includeUnknown)) {
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
        return delegate.getBitmap(idMapping.getReverseId(cur));
      }
    };
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

    Iterable<ImmutableBitmap> getBitmapsInRange(DruidObjectPredicate<String> matcher, int start, int end, boolean includeUnknown)
    {
      return ListFilteredVirtualColumn.getBitmapsInRange(delegate, idMapping, matcher, start, end, includeUnknown);
    }
  }

  private static class ListFilteredNullValueIndex implements NullValueIndex
  {
    private final Supplier<ImmutableBitmap> nullValueBitmapSupplier;

    private ListFilteredNullValueIndex(Supplier<ImmutableBitmap> nullValueBitmapSupplier)
    {
      this.nullValueBitmapSupplier = nullValueBitmapSupplier;
    }

    @Override
    public BitmapColumnIndex get()
    {
      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknowns)
        {
          return bitmapResultFactory.wrapDimensionValue(nullValueBitmapSupplier.get());
        }
      };
    }
  }

  private static class ListFilteredStringValueSetIndexes extends BaseListFilteredColumnIndex
      implements StringValueSetIndexes
  {
    private final Supplier<ImmutableBitmap> nullValueBitmapSupplier;

    private ListFilteredStringValueSetIndexes(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping,
        Supplier<ImmutableBitmap> nullValueBitmapSupplier
    )
    {
      super(delegate, idMapping);
      this.nullValueBitmapSupplier = nullValueBitmapSupplier;
    }

    @Override
    public BitmapColumnIndex forValue(@Nullable String value)
    {
      return new SimpleBitmapColumnIndex()
      {

        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          if (includeUnknown) {
            return bitmapResultFactory.unionDimensionValueBitmaps(
                ImmutableList.of(getBitmapForValue(), nullValueBitmapSupplier.get())
            );
          }
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
      return new SimpleImmutableBitmapDelegatingIterableIndex()
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

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          return nullValueBitmapSupplier.get();
        }
      };
    }
  }

  private static class ListFilteredDruidPredicateIndexes extends BaseListFilteredColumnIndex
      implements DruidPredicateIndexes
  {
    private final Supplier<ImmutableBitmap> nullValueBitmapSupplier;

    private ListFilteredDruidPredicateIndexes(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping,
        Supplier<ImmutableBitmap> nullValueBitmapSupplier
    )
    {
      super(delegate, idMapping);
      this.nullValueBitmapSupplier = nullValueBitmapSupplier;
    }

    @Override
    @Nullable
    public BitmapColumnIndex forPredicate(DruidPredicateFactory matcherFactory)
    {
      return new SimpleBitmapColumnIndex()
      {
        @Override
        public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
        {
          final int start = 0, end = getCardinality();
          Iterable<ImmutableBitmap> bitmaps;
          if (includeUnknown) {
            bitmaps = Iterables.concat(
                getBitmapsInRange(matcherFactory.makeStringPredicate(), start, end, includeUnknown),
                Collections.singletonList(nullValueBitmapSupplier.get())
            );
          } else {
            bitmaps = getBitmapsInRange(matcherFactory.makeStringPredicate(), start, end, includeUnknown);
          }
          return bitmapResultFactory.unionDimensionValueBitmaps(bitmaps);
        }
      };
    }
  }

  private static class ListFilteredLexicographicalRangeIndexes extends BaseListFilteredColumnIndex
      implements LexicographicalRangeIndexes
  {
    private final Supplier<ImmutableBitmap> nullValueBitmapSupplier;

    private ListFilteredLexicographicalRangeIndexes(
        DictionaryEncodedStringValueIndex delegate,
        IdMapping idMapping,
        Supplier<ImmutableBitmap> nullValueBitmapSupplier
    )
    {
      super(delegate, idMapping);
      this.nullValueBitmapSupplier = nullValueBitmapSupplier;
    }

    @Override
    @Nullable
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict
    )
    {
      return forRange(startValue, startStrict, endValue, endStrict, DruidObjectPredicate.alwaysTrue());
    }

    @Override
    @Nullable
    public BitmapColumnIndex forRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        DruidObjectPredicate<String> matcher
    )
    {
      return new SimpleImmutableBitmapDelegatingIterableIndex()
      {
        @Override
        public Iterable<ImmutableBitmap> getBitmapIterable()
        {
          int startIndex, endIndex;
          final int firstValue = NullHandling.isNullOrEquivalent(delegate.getValue(idMapping.getReverseId(0))) ? 1 : 0;
          if (startValue == null) {
            startIndex = firstValue;
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
          return getBitmapsInRange(matcher, start, end, false);
        }

        @Nullable
        @Override
        protected ImmutableBitmap getUnknownsBitmap()
        {
          return nullValueBitmapSupplier.get();
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
    public BitmapFactory getBitmapFactory()
    {
      return delegate.getBitmapFactory();
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return delegate.getBitmap(idMapping.getReverseId(idx));
    }
  }
}
