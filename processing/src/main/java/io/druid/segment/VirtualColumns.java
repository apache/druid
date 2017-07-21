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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.Cacheable;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.virtual.VirtualizedColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class allowing lookup and usage of virtual columns.
 */
public class VirtualColumns implements Cacheable
{
  public static final VirtualColumns EMPTY = new VirtualColumns(
      ImmutableList.of(),
      ImmutableMap.of(),
      ImmutableMap.of()
  );

  /**
   * Split a dot-style columnName into the "main" columnName and the subColumn name after the dot. Useful for
   * columns that support dot notation.
   *
   * @param columnName columnName like "foo" or "foo.bar"
   *
   * @return pair of main column name (will not be null) and subColumn name (may be null)
   */
  public static Pair<String, String> splitColumnName(String columnName)
  {
    final int i = columnName.indexOf('.');
    if (i < 0) {
      return Pair.of(columnName, null);
    } else {
      return Pair.of(columnName.substring(0, i), columnName.substring(i + 1));
    }
  }

  @JsonCreator
  public static VirtualColumns create(List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return EMPTY;
    }
    Map<String, VirtualColumn> withDotSupport = Maps.newHashMap();
    Map<String, VirtualColumn> withoutDotSupport = Maps.newHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (Strings.isNullOrEmpty(vc.getOutputName())) {
        throw new IAE("Empty or null virtualColumn name");
      }

      if (vc.getOutputName().equals(Column.TIME_COLUMN_NAME)) {
        throw new IAE("virtualColumn name[%s] not allowed", vc.getOutputName());
      }

      if (withDotSupport.containsKey(vc.getOutputName()) || withoutDotSupport.containsKey(vc.getOutputName())) {
        throw new IAE("Duplicate virtualColumn name[%s]", vc.getOutputName());
      }

      if (vc.usesDotNotation()) {
        withDotSupport.put(vc.getOutputName(), vc);
      } else {
        withoutDotSupport.put(vc.getOutputName(), vc);
      }
    }
    return new VirtualColumns(ImmutableList.copyOf(virtualColumns), withDotSupport, withoutDotSupport);
  }

  public static VirtualColumns nullToEmpty(@Nullable VirtualColumns virtualColumns)
  {
    return virtualColumns == null ? EMPTY : virtualColumns;
  }

  private VirtualColumns(
      List<VirtualColumn> virtualColumns,
      Map<String, VirtualColumn> withDotSupport,
      Map<String, VirtualColumn> withoutDotSupport
  )
  {
    this.virtualColumns = virtualColumns;
    this.withDotSupport = withDotSupport;
    this.withoutDotSupport = withoutDotSupport;

    for (VirtualColumn virtualColumn : virtualColumns) {
      detectCycles(virtualColumn, null);
    }
  }

  // For equals, hashCode, toString, and serialization:
  private final List<VirtualColumn> virtualColumns;

  // For getVirtualColumn:
  private final Map<String, VirtualColumn> withDotSupport;
  private final Map<String, VirtualColumn> withoutDotSupport;

  public boolean exists(String columnName)
  {
    return getVirtualColumn(columnName) != null;
  }

  public VirtualColumn getVirtualColumn(String columnName)
  {
    final VirtualColumn vc = withoutDotSupport.get(columnName);
    if (vc != null) {
      return vc;
    }
    final String baseColumnName = splitColumnName(columnName).lhs;
    return withDotSupport.get(baseColumnName);
  }

  public ObjectColumnSelector makeObjectColumnSelector(String columnName, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      return null;
    } else {
      return Preconditions.checkNotNull(
          virtualColumn.makeObjectColumnSelector(columnName, factory),
          "VirtualColumn[%s] returned a null ObjectColumnSelector for columnName[%s]",
          virtualColumn.getOutputName(),
          columnName
      );
    }
  }

  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(dimensionSpec.getDimension());
    if (virtualColumn == null) {
      return dimensionSpec.decorate(NullDimensionSelector.instance());
    } else {
      final DimensionSelector selector = virtualColumn.makeDimensionSelector(dimensionSpec, factory);
      return selector == null ? dimensionSpec.decorate(NullDimensionSelector.instance()) : selector;
    }
  }

  public FloatColumnSelector makeFloatColumnSelector(String columnName, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      return ZeroFloatColumnSelector.instance();
    } else {
      final FloatColumnSelector selector = virtualColumn.makeFloatColumnSelector(columnName, factory);
      return selector == null ? ZeroFloatColumnSelector.instance() : selector;
    }
  }

  public LongColumnSelector makeLongColumnSelector(String columnName, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      return ZeroLongColumnSelector.instance();
    } else {
      final LongColumnSelector selector = virtualColumn.makeLongColumnSelector(columnName, factory);
      return selector == null ? ZeroLongColumnSelector.instance() : selector;
    }
  }

  public DoubleColumnSelector makeDoubleColumnSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      return ZeroDoubleColumnSelector.instance();
    } else {
      final DoubleColumnSelector selector = virtualColumn.makeDoubleColumnSelector(columnName, factory);
      return selector == null ? ZeroDoubleColumnSelector.instance() : selector;
    }
  }

  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn != null) {
      return Preconditions.checkNotNull(
          virtualColumn.capabilities(columnName),
          "capabilities for column[%s]",
          columnName
      );
    } else {
      return null;
    }
  }

  public ColumnCapabilities getColumnCapabilitiesWithFallback(StorageAdapter adapter, String columnName)
  {
    final ColumnCapabilities virtualColumnCapabilities = getColumnCapabilities(columnName);
    if (virtualColumnCapabilities != null) {
      return virtualColumnCapabilities;
    } else {
      return adapter.getColumnCapabilities(columnName);
    }
  }

  public boolean isEmpty()
  {
    return withDotSupport.isEmpty() && withoutDotSupport.isEmpty();
  }

  @JsonValue
  public VirtualColumn[] getVirtualColumns()
  {
    // VirtualColumn[] instead of List<VirtualColumn> to aid Jackson serialization.
    return virtualColumns.toArray(new VirtualColumn[]{});
  }

  public ColumnSelectorFactory wrap(final ColumnSelectorFactory baseFactory)
  {
    return new VirtualizedColumnSelectorFactory(baseFactory, this);
  }

  @Override
  public byte[] getCacheKey()
  {
    // id doesn't matter as there is only one kind of "VirtualColumns", so use 0.
    return new CacheKeyBuilder((byte) 0).appendCacheablesIgnoringOrder(virtualColumns).build();
  }

  private void detectCycles(VirtualColumn virtualColumn, Set<String> columnNames)
  {
    // Copy columnNames to avoid modifying it
    final Set<String> nextSet = columnNames == null
                                ? Sets.newHashSet(virtualColumn.getOutputName())
                                : Sets.newHashSet(columnNames);

    for (String columnName : virtualColumn.requiredColumns()) {
      if (!nextSet.add(columnName)) {
        throw new IAE("Self-referential column[%s]", columnName);
      }

      final VirtualColumn dependency = getVirtualColumn(columnName);
      if (dependency != null) {
        detectCycles(dependency, nextSet);
      }
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

    VirtualColumns that = (VirtualColumns) o;

    return virtualColumns.equals(that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return virtualColumns.hashCode();
  }

  @Override
  public String toString()
  {
    return virtualColumns.toString();
  }
}
