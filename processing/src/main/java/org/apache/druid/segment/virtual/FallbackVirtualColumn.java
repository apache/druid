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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A virtual column that picks one column or another based on whether they exist.  It walks through an array of
 * DimensionSpecs finding and using the first column that actually exists.  If it believes that none of them exist
 * it YOLOs it with the first entry from the list.
 * <p>
 * If you are using this virtual column and want to have a decorator/extraction function on your DimensionSpec,
 * it is expected that you will put it on the specs in the list rather than on the spec that references this
 * virtual column.  That is, when this virtual column resolves a dimension, it ignores the decoration from the
 * spec that it was given and instead uses the spec as defined in the list as-is to delegate to the column that
 * it chose.
 */
public class FallbackVirtualColumn implements VirtualColumn
{
  private final String name;
  private final ArrayList<DimensionSpec> columns;

  @JsonCreator
  public FallbackVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("columns") ArrayList<DimensionSpec> columns
  )
  {
    if (columns == null || columns.isEmpty()) {
      throw new IAE("Cannot have a null/empty columns[%s], name[%s]", columns, name);
    }

    this.name = name;
    this.columns = columns;
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty("columns")
  public ArrayList<DimensionSpec> getColumns()
  {
    return columns;
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder bob = new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_LIST_FALLBACK)
        .appendString(name);

    for (DimensionSpec column : columns) {
      bob.appendCacheable(column);
    }

    return bob.build();
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    return factory.makeDimensionSelector(columnToUse(factory));
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    return factory.makeColumnValueSelector(columnToUse(factory).getDimension());
  }

  @Override
  public boolean canVectorize(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    return factory.makeSingleValueDimensionSelector(columnToUse(factory));
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    return factory.makeMultiValueDimensionSelector(columnToUse(factory));
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return factory.makeValueSelector(columnToUse(factory).getDimension());
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    return factory.makeObjectSelector(columnToUse(factory).getDimension());
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createDefault();
  }

  @Nullable
  @SuppressWarnings("ConstantConditions")
  @Override
  public ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return columnToUseInternal(inspector).rhs;
  }

  @Override
  public List<String> requiredColumns()
  {
    ArrayList<String> retVal = new ArrayList<>(columns.size());
    for (DimensionSpec column : columns) {
      retVal.add(column.getDimension());
    }
    return retVal;
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
    final ColumnHolder columnHolder = indexSelector.getColumnHolder(columnToUse(indexSelector).getDimension());
    if (columnHolder == null) {
      return null;
    }
    return columnHolder.getIndexSupplier();
  }

  @SuppressWarnings("ConstantConditions")
  private DimensionSpec columnToUse(ColumnInspector inspector)
  {
    return columnToUseInternal(inspector).lhs;
  }

  @Nonnull
  private Pair<DimensionSpec, ColumnCapabilities> columnToUseInternal(ColumnInspector inspector)
  {
    Iterator<DimensionSpec> specs = columns.iterator();

    DimensionSpec firstSpec = specs.next();
    final ColumnCapabilities firstCapabilities = inspector.getColumnCapabilities(firstSpec.getDimension());

    DimensionSpec spec = firstSpec;
    ColumnCapabilities capabilities = firstCapabilities;
    while (capabilities == null && specs.hasNext()) {
      spec = specs.next();
      capabilities = inspector.getColumnCapabilities(spec.getDimension());
    }

    if (capabilities == null) {
      return Pair.of(firstSpec, firstCapabilities);
    } else {
      return Pair.of(spec, capabilities);
    }
  }
}
