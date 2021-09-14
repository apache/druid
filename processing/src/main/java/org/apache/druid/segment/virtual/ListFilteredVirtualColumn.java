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
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
    if (allowList) {
      return new AllowListBitmapIndex(values, underlyingIndex);
    } else {
      return new DenyListBitmapIndex(values, underlyingIndex);
    }
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

  private abstract static class ListFilteredBitmapIndex implements BitmapIndex
  {
    final Set<String> values;
    final BitmapIndex delegate;

    private ListFilteredBitmapIndex(Set<String> values, BitmapIndex delegate)
    {
      this.values = values;
      this.delegate = delegate;
    }

    @Override
    public String getValue(int index)
    {
      return delegate.getValue(index);
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
    public ImmutableBitmap getBitmap(int idx)
    {
      return delegate.getBitmap(idx);
    }
  }

  private static class AllowListBitmapIndex extends ListFilteredBitmapIndex
  {

    private AllowListBitmapIndex(Set<String> values, BitmapIndex delegate)
    {
      super(values, delegate);
    }

    @Override
    public int getCardinality()
    {
      // cardinality might be smaller than filter if values are not present in the column, but it will not be larger
      return values.size();
    }

    @Override
    public int getIndex(@Nullable String value)
    {
      if (values.contains(value)) {
        return delegate.getIndex(value);
      }
      return -1;
    }
  }

  private static class DenyListBitmapIndex extends ListFilteredBitmapIndex
  {
    private DenyListBitmapIndex(Set<String> values, BitmapIndex delegate)
    {
      super(values, delegate);
    }

    @Override
    public int getCardinality()
    {
      // values in the filter don't necessarily need be present in the column, so report underlying cardinality
      return delegate.getCardinality();
    }

    @Override
    public int getIndex(@Nullable String value)
    {
      if (!values.contains(value)) {
        return delegate.getIndex(value);
      }
      return -1;
    }
  }
}
