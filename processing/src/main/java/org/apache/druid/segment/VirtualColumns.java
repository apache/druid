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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.virtual.VirtualizedColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
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
    Map<String, VirtualColumn> withDotSupport = new HashMap<>();
    Map<String, VirtualColumn> withoutDotSupport = new HashMap<>();
    for (VirtualColumn vc : virtualColumns) {
      if (Strings.isNullOrEmpty(vc.getOutputName())) {
        throw new IAE("Empty or null virtualColumn name");
      }

      if (vc.getOutputName().equals(ColumnHolder.TIME_COLUMN_NAME)) {
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

  /**
   * Returns true if a virtual column exists with a particular columnName.
   *
   * @param columnName the column name
   *
   * @return true or false
   */
  public boolean exists(String columnName)
  {
    return getVirtualColumn(columnName) != null;
  }

  @Nullable
  public VirtualColumn getVirtualColumn(String columnName)
  {
    final VirtualColumn vc = withoutDotSupport.get(columnName);
    if (vc != null) {
      return vc;
    }
    final String baseColumnName = splitColumnName(columnName).lhs;
    return withDotSupport.get(baseColumnName);
  }

  /**
   * Create a dimension (string) selector.
   *
   * @param dimensionSpec the dimensionSpec for this selector
   * @param factory       base column selector factory
   *
   * @return selector
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(dimensionSpec.getDimension());
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", dimensionSpec.getDimension());
    } else {
      final DimensionSelector selector = virtualColumn.makeDimensionSelector(dimensionSpec, factory);
      Preconditions.checkNotNull(selector, "selector");
      return selector;
    }
  }

  @Nullable
  public BitmapIndex getBitmapIndex(String columnName, ColumnSelector columnSelector)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", columnName);
    } else {
      return virtualColumn.capabilities(columnName).hasBitmapIndexes() ? virtualColumn.getBitmapIndex(
          columnName,
          columnSelector
      ) : null;
    }
  }

  @Nullable
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumn(dimensionSpec.getDimension());
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", dimensionSpec.getDimension());
    } else {
      return virtualColumn.makeDimensionSelector(dimensionSpec, columnSelector, offset);
    }
  }

  @Nullable
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", columnName);
    } else {
      return virtualColumn.makeColumnValueSelector(columnName, columnSelector, offset);
    }
  }

  /**
   * Create a column value selector.
   *
   * @param columnName column mame
   * @param factory    base column selector factory
   *
   * @return selector
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", columnName);
    } else {
      final ColumnValueSelector<?> selector = virtualColumn.makeColumnValueSelector(columnName, factory);
      Preconditions.checkNotNull(selector, "selector");
      return selector;
    }
  }

  @Nullable
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

  @Nullable
  public ColumnCapabilities getColumnCapabilitiesWithFallback(StorageAdapter adapter, String columnName)
  {
    final ColumnCapabilities virtualColumnCapabilities = getColumnCapabilities(columnName);
    if (virtualColumnCapabilities != null) {
      return virtualColumnCapabilities;
    } else {
      return adapter.getColumnCapabilities(columnName);
    }
  }

  @JsonValue
  public VirtualColumn[] getVirtualColumns()
  {
    // VirtualColumn[] instead of List<VirtualColumn> to aid Jackson serialization.
    return virtualColumns.toArray(new VirtualColumn[0]);
  }

  public int size()
  {
    return virtualColumns.size();
  }

  public ColumnSelectorFactory wrap(final ColumnSelectorFactory baseFactory)
  {
    if (virtualColumns.isEmpty()) {
      return baseFactory;
    } else {
      return new VirtualizedColumnSelectorFactory(baseFactory, this);
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    // id doesn't matter as there is only one kind of "VirtualColumns", so use 0.
    return new CacheKeyBuilder((byte) 0).appendCacheablesIgnoringOrder(virtualColumns).build();
  }

  private void detectCycles(VirtualColumn virtualColumn, @Nullable Set<String> columnNames)
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
