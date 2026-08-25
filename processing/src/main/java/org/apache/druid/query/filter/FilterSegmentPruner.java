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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
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
  private final Map<VirtualColumns.Node, Optional<VirtualColumn>> shardEquivalenceCache;

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

    if (shard != null) {
      final Map<String, RangeSet<String>> filterDomain = new HashMap<>();
      for (String dimension : shard.getDomainDimensions()) {
        addToFilterDomain(dimension, shard.getDomainVirtualColumns(), filterDomain);
      }
      if (!filterDomain.isEmpty() && !shard.possibleInDomain(filterDomain)) {
        return false;
      }
    }

    final ClusterGroupTuples clusterGroups = segment.getClusterGroups();
    if (clusterGroups != null && !possibleInClusterGroups(clusterGroups)) {
      return false;
    }

    return true;
  }

  private boolean possibleInClusterGroups(ClusterGroupTuples clusterGroups)
  {
    final RowSignature clusteringColumns = clusterGroups.clusteringColumns();
    final int numColumns = clusteringColumns.size();

    final Map<String, RangeSet<String>> filterDomain = new HashMap<>();
    for (int i = 0; i < numColumns; i++) {
      final String column = clusteringColumns.getColumnName(i);
      if (!ColumnType.STRING.equals(clusteringColumns.getColumnType(i).orElse(null))) {
        continue;
      }
      addToFilterDomain(column, clusterGroups.virtualColumns(), filterDomain);
    }

    if (filterDomain.isEmpty()) {
      // Filter doesn't constrain any string clustering column.
      return true;
    }

    for (final List<Object> tuple : clusterGroups.tuples()) {
      if (tupleMatchesDomain(clusteringColumns, tuple, filterDomain)) {
        return true;
      }
    }

    return false;
  }

  private static boolean tupleMatchesDomain(
      RowSignature clusteringColumns,
      List<Object> tuple,
      Map<String, RangeSet<String>> filterDomain
  )
  {
    for (int i = 0; i < clusteringColumns.size(); i++) {
      final RangeSet<String> domainRangeSet = filterDomain.get(clusteringColumns.getColumnName(i));
      if (domainRangeSet == null) {
        continue;
      }
      final Object rawValue = tuple.get(i);
      // Nulls are less than empty String in segments
      final Range<String> valueRange = rawValue == null ? Range.lessThan("") : Range.singleton((String) rawValue);
      if (domainRangeSet.subRangeSet(valueRange).isEmpty()) {
        return false;
      }
    }
    return true;
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

  /**
   * Adds the filter's {@link RangeSet} for {@code column} to {@code filterDomain}, resolving through
   * {@code domainVirtualColumns} to the query's equivalent virtual column if {@code column} is virtual there.
   */
  private void addToFilterDomain(
      String column,
      VirtualColumns domainVirtualColumns,
      Map<String, RangeSet<String>> filterDomain
  )
  {
    final VirtualColumns.Node domainNode = domainVirtualColumns.getNode(column);
    if (domainNode != null) {
      final VirtualColumn queryEquivalent = getQueryEquivalent(domainNode);
      if (queryEquivalent != null && filterFields.contains(queryEquivalent.getOutputName())) {
        final Optional<RangeSet<String>> optFilterRangeSet = rangeCache.computeIfAbsent(
            queryEquivalent.getOutputName(),
            d -> Optional.ofNullable(filter.getDimensionRangeSet(d))
        );
        optFilterRangeSet.ifPresent(rangeSet -> filterDomain.put(column, rangeSet));
      }
    } else if (filterFields.contains(column)) {
      final Optional<RangeSet<String>> optFilterRangeSet =
          rangeCache.computeIfAbsent(column, d -> Optional.ofNullable(filter.getDimensionRangeSet(d)));
      optFilterRangeSet.ifPresent(rangeSet -> filterDomain.put(column, rangeSet));
    }
  }

  @Nullable
  private VirtualColumn getQueryEquivalent(VirtualColumns.Node node)
  {
    final Optional<VirtualColumn> cached = shardEquivalenceCache.computeIfAbsent(
        node,
        n -> Optional.ofNullable(virtualColumns.findEquivalent(n))
    );
    return cached.orElse(null);
  }
}
