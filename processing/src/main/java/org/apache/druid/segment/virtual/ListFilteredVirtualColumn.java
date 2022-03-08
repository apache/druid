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
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdMapping;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

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

  @Override
  public @Nullable BitmapIndex getBitmapIndex(
      String columnName,
      ColumnSelector selector
  )
  {
    final ColumnHolder holder = selector.getColumnHolder(delegate.getDimension());
    if (holder == null) {
      return null;
    }
    final BitmapIndex underlyingIndex = holder.getBitmapIndex();
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

    return new ListFilteredBitmapIndex(underlyingIndex, idMapping);
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

  private static class ListFilteredBitmapIndex implements BitmapIndex
  {
    final BitmapIndex delegate;
    final IdMapping idMapping;

    private ListFilteredBitmapIndex(BitmapIndex delegate, IdMapping idMapping)
    {
      this.delegate = delegate;
      this.idMapping = idMapping;
    }

    @Override
    public String getValue(int index)
    {
      return delegate.getValue(idMapping.getReverseId(index));
    }

    @Override
    public boolean hasNulls()
    {
      return delegate.hasNulls();
    }

    @Override
    public BitmapFactory getBitmapFactory()
    {
      return delegate.getBitmapFactory();
    }

    @Override
    public int getCardinality()
    {
      return idMapping.getValueCardinality();
    }

    @Override
    public int getIndex(@Nullable String value)
    {
      return getReverseIndex(value);
    }

    @Override
    public ImmutableBitmap getBitmap(int idx)
    {
      return delegate.getBitmap(idMapping.getReverseId(idx));
    }

    @Override
    public ImmutableBitmap getBitmapForValue(@Nullable String value)
    {
      if (getReverseIndex(value) < 0) {
        return delegate.getBitmap(-1);
      }
      return delegate.getBitmapForValue(value);
    }

    @Override
    public Iterable<ImmutableBitmap> getBitmapsInRange(
        @Nullable String startValue,
        boolean startStrict,
        @Nullable String endValue,
        boolean endStrict,
        Predicate<String> matcher
    )
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

      endIndex = startIndex > endIndex ? startIndex : endIndex;
      final int start = startIndex, end = endIndex;
      return () -> new Iterator<ImmutableBitmap>()
      {
        int currIndex = start;
        int found;
        {
          found = findNext();
        }

        private int findNext()
        {
          while (currIndex < end && !matcher.test(delegate.getValue(idMapping.getReverseId(currIndex)))) {
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

    @Override
    public Iterable<ImmutableBitmap> getBitmapsForValues(Set<String> values)
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

    private int getReverseIndex(@Nullable String value)
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
  }
}
