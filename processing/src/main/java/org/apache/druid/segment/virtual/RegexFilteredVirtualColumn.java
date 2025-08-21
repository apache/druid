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
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Virtual column form of {@link RegexFilteredDimensionSpec}
 */
public class RegexFilteredVirtualColumn implements VirtualColumn
{
  private final String name;
  private final DimensionSpec delegate;
  private final String pattern;

  @JsonCreator
  public RegexFilteredVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("pattern") String pattern
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.pattern = Preconditions.checkNotNull(pattern, "pattern");
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public String getPattern()
  {
    return pattern;
  }

  @JsonProperty
  public DimensionSpec getDelegate()
  {
    return delegate;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder builder = new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_REGEX_FILTERED)
        .appendString(name)
        .appendCacheable(delegate)
        .appendString(pattern);
    return builder.build();
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    return new RegexFilteredDimensionSpec(
        delegate,
        pattern
    ).decorate(factory.makeDimensionSelector(delegate));
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
    return new ColumnCapabilitiesImpl()
        .setType(delegate.getOutputType())
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegexFilteredVirtualColumn that = (RegexFilteredVirtualColumn) o;
    return name.equals(that.name) &&
           delegate.equals(that.delegate) &&
           pattern.equals(that.pattern);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, delegate, pattern);
  }

  @Override
  public String toString()
  {
    return "RegexFilteredVirtualColumn{" +
           "name='" + name + '\'' +
           ", delegate=" + delegate +
           ", pattern='" + pattern + '\'' +
           '}';
  }
}
