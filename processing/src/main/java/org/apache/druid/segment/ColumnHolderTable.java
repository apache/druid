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

import com.google.common.base.Supplier;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.segment.column.BaseColumnHolder;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Column holders of a {@link SimpleQueryableIndex}, looked up by name. Column names and positions live in a
 * {@link Layout}, which is interned, so tables with the same column list share one. Each table itself holds only an
 * array with one slot per column.
 */
public class ColumnHolderTable
{
  private static final Interner<Layout> LAYOUT_INTERNER = Interners.newWeakInterner();

  private final Layout layout;

  /**
   * Each slot holds a {@link BaseColumnHolder}, or a {@link Supplier} of one for columns that are loaded lazily.
   */
  private final Object[] slots;

  private ColumnHolderTable(Layout layout, Object[] slots)
  {
    this.layout = layout;
    this.slots = slots;
  }

  /**
   * Returns a builder for a new table.
   */
  static Builder builder()
  {
    return new Builder();
  }

  /**
   * Creates a table from a map of column suppliers, in the map's iteration order.
   */
  static ColumnHolderTable fromSupplierMap(Map<String, ? extends Supplier<BaseColumnHolder>> columns)
  {
    final Builder builder = builder();
    for (Map.Entry<String, ? extends Supplier<BaseColumnHolder>> entry : columns.entrySet()) {
      builder.putSupplier(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  /**
   * Returns a builder that starts with the columns of this table, in table order.
   */
  Builder toBuilder()
  {
    final Builder builder = builder();
    for (int i = 0; i < slots.length; i++) {
      builder.columns.put(layout.names.get(i), slots[i]);
    }
    return builder;
  }

  /**
   * Returns the holder for the given column, or null if there is no such column.
   */
  @Nullable
  BaseColumnHolder get(String columnName)
  {
    final int position = layout.indexOf(columnName);
    return position < 0 ? null : resolve(slots[position]);
  }

  /**
   * Returns whether the table has the given column, without loading it.
   */
  boolean contains(String columnName)
  {
    return layout.indexOf(columnName) >= 0;
  }

  boolean isEmpty()
  {
    return slots.length == 0;
  }

  /**
   * Returns column names in table order.
   */
  List<String> getColumnNames()
  {
    return layout.names;
  }

  /**
   * Returns a new mutable map of column name to holder supplier, in table order.
   */
  Map<String, Supplier<BaseColumnHolder>> toSupplierMap()
  {
    final Map<String, Supplier<BaseColumnHolder>> map = new LinkedHashMap<>();
    for (int i = 0; i < slots.length; i++) {
      map.put(layout.names.get(i), toSupplier(slots[i]));
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private static BaseColumnHolder resolve(Object slot)
  {
    if (slot instanceof BaseColumnHolder holder) {
      return holder;
    } else {
      return ((Supplier<BaseColumnHolder>) slot).get();
    }
  }

  @SuppressWarnings("unchecked")
  private static Supplier<BaseColumnHolder> toSupplier(Object slot)
  {
    if (slot instanceof BaseColumnHolder holder) {
      return () -> holder;
    } else {
      return (Supplier<BaseColumnHolder>) slot;
    }
  }

  /**
   * Builds a {@link ColumnHolderTable}. Columns are kept in insertion order.
   */
  static final class Builder
  {
    private final LinkedHashMap<String, Object> columns = new LinkedHashMap<>();

    private Builder()
    {
    }

    /**
     * Adds a column that has already been loaded.
     */
    Builder put(String columnName, BaseColumnHolder holder)
    {
      columns.put(columnName, holder);
      return this;
    }

    /**
     * Adds a column that is loaded on first access. The supplier must memoize.
     */
    Builder putSupplier(String columnName, Supplier<BaseColumnHolder> supplier)
    {
      columns.put(columnName, supplier);
      return this;
    }

    /**
     * Renames a column, moving it to the end of the table.
     */
    void rename(String oldName, String newName)
    {
      columns.put(newName, columns.remove(oldName));
    }

    ColumnHolderTable build()
    {
      final Layout layout = LAYOUT_INTERNER.intern(new Layout(List.copyOf(columns.keySet())));
      return new ColumnHolderTable(layout, columns.values().toArray());
    }
  }

  /**
   * Column names of a table, and the position of each name. Positions are found by binary search, which needs less
   * memory than a hash map. Typically interned.
   */
  private static final class Layout
  {
    private final List<String> names;

    /**
     * Positions in {@link #names}, ordered by name. Computed on first use, so that layouts discarded by
     * {@link #LAYOUT_INTERNER} in favor of an equal one are never sorted.
     */
    @Nullable
    private volatile int[] sortedPositions;

    private Layout(List<String> names)
    {
      this.names = names;
    }

    /**
     * Returns the position of the given name, or -1 if there is no such name.
     */
    private int indexOf(@Nullable String name)
    {
      if (name == null) {
        return -1;
      }
      final int[] positions = getSortedPositions();
      int low = 0;
      int high = positions.length - 1;
      while (low <= high) {
        final int mid = (low + high) >>> 1;
        final int cmp = names.get(positions[mid]).compareTo(name);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return positions[mid];
        }
      }
      return -1;
    }

    /**
     * Returns {@link #sortedPositions}, computing it if needed. Racing threads compute equal arrays, so it does not
     * matter which one is kept.
     */
    private int[] getSortedPositions()
    {
      int[] positions = sortedPositions;
      if (positions == null) {
        positions = new int[names.size()];
        for (int i = 0; i < positions.length; i++) {
          positions[i] = i;
        }
        IntArrays.quickSort(positions, (i1, i2) -> names.get(i1).compareTo(names.get(i2)));
        sortedPositions = positions;
      }
      return positions;
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
      return names.equals(((Layout) o).names);
    }

    @Override
    public int hashCode()
    {
      return names.hashCode();
    }
  }
}
