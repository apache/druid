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

package org.apache.druid.query.filter;

import com.google.common.collect.RangeSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
  * Uses a {@link DimFilter} to check the {@link DimFilter#getDimensionRangeSet(String)} against
 * {@link ShardSpec#possibleInDomain(Map)} in order to 'prune' a set of segments whose rows would never match a filter
 * and avoid processing those segments in the first place.
  */
public class FilterSegmentPruner implements SegmentPruner
{
  private final DimFilter filter;
  private final Set<String> filterFields;
  private final VirtualColumns virtualColumns;
  private final Map<String, Optional<RangeSet<String>>> rangeCache;
  private final Map<ShardVirtualColumnCacheEntry, Optional<VirtualColumn>> shardEquivalenceCache;

  public FilterSegmentPruner(
      DimFilter filter,
      @Nullable Set<String> filterFields,
      @Nullable VirtualColumns virtualColumns
  )
  {
    this.filter = InvalidInput.notNull(filter, "filter");
    this.filterFields = filterFields == null ? filter.getRequiredColumns() : filterFields;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.rangeCache = new HashMap<>();
    this.shardEquivalenceCache = new HashMap<>();
  }


  /**
   * Returns false if the {@link DataSegment} does not fit in {@link DimFilter#getDimensionRangeSet(String)}.
   * <p>
   * {@link #rangeCache} stores the RangeSets of different dimensions for the filter, so it can be re-used between
   * calls to save redundant evaluation of {@link DimFilter#getDimensionRangeSet(String)} on the same columns.
   */
  @Override
  public boolean include(DataSegment segment)
  {
    final ShardSpec shard = segment.getShardSpec();
    boolean include = true;

    if (shard != null) {
      final Map<String, RangeSet<String>> filterDomain = new HashMap<>();
      final List<String> dimensions = shard.getDomainDimensions();
      for (String dimension : dimensions) {
        final VirtualColumn shardVirtualColumn = shard.getDomainVirtualColumns().getVirtualColumn(dimension);
        if (shardVirtualColumn != null) {
          final VirtualColumn queryEquivalent = getQueryEquivalent(
              shard.getDomainVirtualColumns(),
              shardVirtualColumn
          );
          if (queryEquivalent != null) {
            if (filterFields == null || filterFields.contains(queryEquivalent.getOutputName())) {
              final Optional<RangeSet<String>> optFilterRangeSet = rangeCache
                  .computeIfAbsent(
                      queryEquivalent.getOutputName(),
                      d -> Optional.ofNullable(filter.getDimensionRangeSet(d))
                  );
              optFilterRangeSet.ifPresent(stringRangeSet -> filterDomain.put(
                  shardVirtualColumn.getOutputName(),
                  stringRangeSet
              ));
            }
          }
        } else if (filterFields == null || filterFields.contains(dimension)) {
          final Optional<RangeSet<String>> optFilterRangeSet =
              rangeCache.computeIfAbsent(dimension, d -> Optional.ofNullable(filter.getDimensionRangeSet(d)));
          optFilterRangeSet.ifPresent(stringRangeSet -> filterDomain.put(dimension, stringRangeSet));
        }
      }
      if (!filterDomain.isEmpty() && !shard.possibleInDomain(filterDomain)) {
        include = false;
      }
    }
    return include;
  }

  @Override
  public SegmentPruner combine(SegmentPruner other)
  {
    if (other instanceof FilterSegmentPruner pruner) {
      final List<VirtualColumn> combinedVirtualColumns = new ArrayList<>();
      combinedVirtualColumns.addAll(List.of(virtualColumns.getVirtualColumns()));
      combinedVirtualColumns.addAll(List.of(pruner.virtualColumns.getVirtualColumns()));

      final Set<String> combinedFields = new LinkedHashSet<>();
      combinedFields.addAll(filterFields);
      combinedFields.addAll(pruner.filterFields);

      final DimFilter combinedFilter = new AndDimFilter(filter, pruner.filter);

      return new FilterSegmentPruner(
          combinedFilter,
          combinedFields,
          VirtualColumns.create(combinedVirtualColumns)
      );
    } else if (other instanceof CompositeSegmentPruner composite) {
      // composite pruner can combine a filter pruner with any filter pruners it already has, so call it
      return composite.combine(this);
    }
    return new CompositeSegmentPruner(
        Set.of(this, other)
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FilterSegmentPruner that = (FilterSegmentPruner) o;
    return Objects.equals(filter, that.filter) &&
           Objects.equals(filterFields, that.filterFields) &&
           Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(filter, filterFields, virtualColumns);
  }

  @Override
  public String toString()
  {
    return "FilterSegmentPruner{" +
           "filter=" + filter +
           ", filterFields=" + filterFields +
           ", virtualColumns=" + virtualColumns +
           '}';
  }

  @Nullable
  private VirtualColumn getQueryEquivalent(VirtualColumns shardVirtualColumns, VirtualColumn shardVirtualColumn)
  {
    final Optional<VirtualColumn> cached = shardEquivalenceCache.computeIfAbsent(
        new ShardVirtualColumnCacheEntry(shardVirtualColumn, shardVirtualColumns),
        virtualColumn -> Optional.ofNullable(virtualColumns.findEquivalent(shardVirtualColumns, virtualColumn.shardVirtualColumn))
    );
    return cached.orElse(null);
  }

  /**
   * Structure to preserve the VirtualColumn 'tree' to use as a cache key so that we can distinguish otherwise
   * identical {@link VirtualColumn} that depend on other virtual columns that have the same name but are different
   */
  private static final class ShardVirtualColumnCacheEntry
  {
    private final VirtualColumn shardVirtualColumn;
    private final List<ShardVirtualColumnCacheEntry> dependents;

    public ShardVirtualColumnCacheEntry(VirtualColumn shardVirtualColumn, VirtualColumns shardVirtualColumns)
    {
      this.shardVirtualColumn = shardVirtualColumn;
      this.dependents = new ArrayList<>();
      for (String required : shardVirtualColumn.requiredColumns()) {
        final VirtualColumn dependent = shardVirtualColumns.getVirtualColumn(required);
        if (dependent != null) {
          dependents.add(new ShardVirtualColumnCacheEntry(dependent, shardVirtualColumns));
        }
      }
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ShardVirtualColumnCacheEntry that = (ShardVirtualColumnCacheEntry) o;
      return Objects.equals(shardVirtualColumn, that.shardVirtualColumn) &&
             Objects.equals(dependents, that.dependents);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(shardVirtualColumn, dependents);
    }
  }
}
