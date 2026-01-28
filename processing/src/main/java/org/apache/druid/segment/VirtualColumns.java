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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.VirtualizedColumnInspector;
import org.apache.druid.segment.virtual.VirtualizedColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
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
  public static VirtualColumns create(@Nullable List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return EMPTY;
    }
    return fromIterable(virtualColumns);
  }

  public static VirtualColumns create(VirtualColumn... virtualColumns)
  {
    return create(Arrays.asList(virtualColumns));
  }

  public static VirtualColumns fromIterable(Iterable<VirtualColumn> virtualColumns)
  {
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

  // For equals, hashCode, toString, and serialization:
  private final List<VirtualColumn> virtualColumns;
  private final List<String> virtualColumnNames;
  // For equivalence
  private final Supplier<Map<VirtualColumn.EquivalenceKey, VirtualColumn>> equivalence;

  // For getVirtualColumn:
  private final Map<String, VirtualColumn> withDotSupport;
  private final Map<String, VirtualColumn> withoutDotSupport;
  private final boolean hasNoDotColumns;

  private VirtualColumns(
      List<VirtualColumn> virtualColumns,
      Map<String, VirtualColumn> withDotSupport,
      Map<String, VirtualColumn> withoutDotSupport
  )
  {
    this.virtualColumns = virtualColumns;
    this.withDotSupport = withDotSupport;
    this.withoutDotSupport = withoutDotSupport;
    this.virtualColumnNames = new ArrayList<>(virtualColumns.size());
    this.hasNoDotColumns = withDotSupport.isEmpty();
    for (VirtualColumn virtualColumn : virtualColumns) {
      detectCycles(virtualColumn, null);
      virtualColumnNames.add(virtualColumn.getOutputName());
    }
    this.equivalence = Suppliers.memoize(() -> {
      final Map<VirtualColumn.EquivalenceKey, VirtualColumn> equiv =
          Maps.newHashMapWithExpectedSize(virtualColumns.size());
      for (VirtualColumn virtualColumn : virtualColumns) {
        final VirtualColumn.EquivalenceKey key = virtualColumn.getEquivalanceKey();
        if (key != null) {
          equiv.putIfAbsent(key, virtualColumn);
        }
      }
      return equiv;
    });
  }

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
    if (hasNoDotColumns) {
      return null;
    }
    final String baseColumnName = splitColumnName(columnName).lhs;
    return withDotSupport.get(baseColumnName);
  }

  /**
   * Check if a virtual column is already defined which is the same as some other virtual column, ignoring output name,
   * returning that virtual column if it exists, or null if there is no equivalent virtual column.
   */
  @Nullable
  public VirtualColumn findEquivalent(VirtualColumn virtualColumn)
  {
    return equivalence.get().get(virtualColumn.getEquivalanceKey());
  }

  /**
   * Get the {@link ColumnIndexSupplier} of the specified virtual column, with the assistance of a
   * {@link ColumnSelector} to allow reading things from segments. Returns null if the virtual column wants to
   * act like a missing column. Returns {@link NoIndexesColumnIndexSupplier#getInstance()} if the virtual
   * column does not support indexes and wants cursor-based filtering.
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  @Nullable
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector columnIndexSelector
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(columnName);
    return virtualColumn.getIndexSupplier(columnName, columnIndexSelector);
  }

  /**
   * Create a dimension (string) selector.
   *
   * @param dimensionSpec   spec the column was referenced with
   * @param selectorFactory object for fetching underlying selectors.
   * @param columnSelector  object for fetching underlying columns, if available.
   * @param offset          offset to use with underlying columns. Must be provided if columnSelector is provided.
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory selectorFactory,
      @Nullable ColumnSelector columnSelector,
      @Nullable ReadableOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(dimensionSpec.getDimension());
    final DimensionSelector selector =
        virtualColumn.makeDimensionSelector(dimensionSpec, selectorFactory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }

  /**
   * Create a column value selector.
   *
   * @param columnName      name the column was referenced with, which is useful if this column uses dot notation.
   * @param selectorFactory object for fetching underlying selectors.
   * @param columnSelector  object for fetching underlying columns, if available.
   * @param offset          offset to use with underlying columns. Must be provided if columnSelector is provided.
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory selectorFactory,
      @Nullable ColumnSelector columnSelector,
      @Nullable ReadableOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(columnName);
    final ColumnValueSelector<?> selector =
        virtualColumn.makeColumnValueSelector(columnName, selectorFactory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }

  public boolean canVectorize(ColumnInspector columnInspector)
  {
    final ColumnInspector inspector = wrapInspector(columnInspector);
    for (VirtualColumn virtualColumn : virtualColumns) {
      if (!virtualColumn.canVectorize(inspector)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a single value dimension vector (string) selector.
   *
   * @param dimensionSpec  spec the column was referenced with
   * @param factory        object for fetching underlying selectors
   * @param columnSelector object for fetching underlying columns
   * @param offset         offset to use with underlying columns
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(dimensionSpec.getDimension());
    final SingleValueDimensionVectorSelector selector =
        virtualColumn.makeSingleValueVectorDimensionSelector(dimensionSpec, factory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }

  /**
   * Create a multi value dimension vector (string) selector.
   *
   * @param dimensionSpec  spec the column was referenced with
   * @param factory        object for fetching underlying selectors
   * @param columnSelector object for fetching underlying columns
   * @param offset         offset to use with underlying columns
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(dimensionSpec.getDimension());
    final MultiValueDimensionVectorSelector selector =
        virtualColumn.makeMultiValueVectorDimensionSelector(dimensionSpec, factory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }


  /**
   * Create a column vector value selector.
   *
   * @param columnName     name the column was referenced with
   * @param factory        object for fetching underlying selectors
   * @param columnSelector object for fetching underlying columns
   * @param offset         offset to use with underlying columns
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public VectorValueSelector makeVectorValueSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(columnName);
    final VectorValueSelector selector =
        virtualColumn.makeVectorValueSelector(columnName, factory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }

  /**
   * Create a column vector object selector.
   *
   * @param columnName     name the column was referenced with
   * @param factory        object for fetching underlying selectors
   * @param columnSelector object for fetching underlying columns
   * @param offset         offset to use with underlying columns
   *
   * @throws IllegalArgumentException if the virtual column does not exist (see {@link #exists(String)}
   */
  public VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    final VirtualColumn virtualColumn = getVirtualColumnForSelector(columnName);
    final VectorObjectSelector selector =
        virtualColumn.makeVectorObjectSelector(columnName, factory, columnSelector, offset);
    Preconditions.checkNotNull(selector, "selector");
    return selector;
  }

  /**
   * Get capabilities for the virtual column "columnName". If columnName is not a virtual column, returns null.
   * Package-private since production callers want {@link #getColumnCapabilitiesWithFallback(ColumnInspector, String)}.
   */
  @Nullable
  ColumnCapabilities getColumnCapabilitiesWithoutFallback(ColumnInspector inspector, String columnName)
  {
    final VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn != null) {
      return virtualColumn.capabilities(column -> getColumnCapabilitiesWithFallback(inspector, column), columnName);
    } else {
      return null;
    }
  }

  /**
   * Get capabilities for the column "columnName". If columnName is not a virtual column, delegates to the
   * provided {@link ColumnInspector}.
   */
  @Nullable
  public ColumnCapabilities getColumnCapabilitiesWithFallback(ColumnInspector inspector, String columnName)
  {
    final ColumnCapabilities virtualColumnCapabilities = getColumnCapabilitiesWithoutFallback(inspector, columnName);
    if (virtualColumnCapabilities != null) {
      return virtualColumnCapabilities;
    } else {
      return inspector.getColumnCapabilities(columnName);
    }
  }

  @JsonValue
  public VirtualColumn[] getVirtualColumns()
  {
    // VirtualColumn[] instead of List<VirtualColumn> to aid Jackson serialization.
    return virtualColumns.toArray(new VirtualColumn[0]);
  }

  /**
   * Creates a {@link VirtualizedColumnSelectorFactory} which can create column selectors for {@link #virtualColumns}
   * in addition to selectors for all physical columns in the underlying factory.
   */
  public ColumnSelectorFactory wrap(final ColumnSelectorFactory baseFactory)
  {
    if (virtualColumns.isEmpty()) {
      return baseFactory;
    } else {
      return new VirtualizedColumnSelectorFactory(baseFactory, this);
    }
  }

  /**
   * Creates a {@link VirtualizedColumnInspector} that provides {@link ColumnCapabilities} information for all
   * {@link #virtualColumns} in addition to the capabilities of all physical columns in the underlying inspector.
   */
  public ColumnInspector wrapInspector(ColumnInspector inspector)
  {
    if (virtualColumns.isEmpty()) {
      return inspector;
    } else {
      return new VirtualizedColumnInspector(inspector, this);
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    // id doesn't matter as there is only one kind of "VirtualColumns", so use 0.
    return new CacheKeyBuilder((byte) 0).appendCacheablesIgnoringOrder(virtualColumns).build();
  }

  public boolean isEmpty()
  {
    return virtualColumns.isEmpty();
  }

  public List<String> getColumnNames()
  {
    return virtualColumnNames;
  }

  private VirtualColumn getVirtualColumnForSelector(String columnName)
  {
    VirtualColumn virtualColumn = getVirtualColumn(columnName);
    if (virtualColumn == null) {
      throw new IAE("No such virtual column[%s]", columnName);
    }
    return virtualColumn;
  }

  /**
   * Detects cycles in the dependencies of a {@link VirtualColumn}.
   *
   * @param virtualColumn virtual column to check
   * @param visited       null on initial call. Internally, this method operates recursively, and uses this parameter
   *                      to pass down the list of already-visited columns.
   */
  private void detectCycles(VirtualColumn virtualColumn, @Nullable Set<String> visited)
  {
    // Copy "visited" to avoid modifying it
    final Set<String> visitedCopy = visited == null
                                    ? Sets.newHashSet(virtualColumn.getOutputName())
                                    : Sets.newHashSet(visited);

    for (String columnName : virtualColumn.requiredColumns()) {
      final VirtualColumn dependency = getVirtualColumn(columnName);
      if (dependency != null) {
        if (!visitedCopy.add(columnName)) {
          throw new IAE("Self-referential column[%s]", columnName);
        }
        detectCycles(dependency, visitedCopy);
        visitedCopy.remove(columnName);
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

  /**
   * {@link JsonInclude} filter for {@code getVirtualColumns()}.
   *
   * This API works by "creative" use of equals. It requires warnings to be suppressed
   * and also requires spotbugs exclusions (see spotbugs-exclude.xml).
   */
  @SuppressWarnings({"EqualsAndHashcode", "EqualsHashCode"})
  public static class JsonIncludeFilter // lgtm [java/inconsistent-equals-and-hashcode]
  {
    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof VirtualColumns &&
             ((VirtualColumns) obj).virtualColumns.isEmpty();
    }
  }
}
