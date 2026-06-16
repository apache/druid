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

package org.apache.druid.segment.incremental;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One cluster group within an {@link OnHeapClusteredBaseTable}: a sub-IncrementalIndex holding the non-clustering
 * rows for a single clustering tuple. The clustering values themselves are NOT stored in this group's facts
 * holder — they're constants captured on {@link #clusteringValues} (per the share-nothing v1 design, clustering
 * values are written once on the on-disk group spec rather than per-row).
 * <p>
 * Each group owns its own {@link IncrementalIndex.DimensionDesc} list for the non-clustering dimensions, which
 * means own {@link DimensionIndexer} instances and own dictionaries. That isolation is
 * what gives the persisted segment the "share nothing across groups" property — at persist time each group's
 * dictionaries are written under its per-group file-bundle prefix.
 * <p>
 * Clustered base tables are never rollup and have no metric columns. When rollup is enabled, the facts holder
 * deduplicates rows within a group (never across groups, since two rows with different clustering tuples land in
 * different groups by construction); with no aggregators a rollup hit is simply a no-op dedup.
 */
public final class OnHeapClusterGroup implements IncrementalIndexRowSelector
{
  private final OnHeapClusteredBaseTable parent;
  private final Object[] clusteringValues;
  private final List<Integer> clusteringValueIds;

  private final List<IncrementalIndex.DimensionDesc> dimensions;
  private final Map<String, IncrementalIndex.DimensionDesc> dimensionsMap;
  private final Map<String, ColumnFormat> columnFormats;
  private final FactsHolder factsHolder;
  private final AtomicInteger rowCounter = new AtomicInteger(0);
  private final AtomicInteger numEntries = new AtomicInteger(0);
  private final int groupTimePosition;

  private final VirtualColumns virtualColumns;
  private final ColumnSelectorFactory virtualSelectorFactory;

  OnHeapClusterGroup(
      OnHeapClusteredBaseTable parent,
      Object[] clusteringValues,
      List<Integer> clusteringValueIds,
      List<DimensionSchema> nonClusteringDimensions,
      VirtualColumns virtualColumns,
      IncrementalIndex.InputRowHolder inputRowHolder,
      boolean rollup,
      int timePosition
  )
  {
    this.parent = parent;
    this.clusteringValues = clusteringValues;
    this.clusteringValueIds = Collections.unmodifiableList(new ArrayList<>(clusteringValueIds));

    this.dimensions = new ArrayList<>(nonClusteringDimensions.size());
    this.dimensionsMap = new LinkedHashMap<>();
    this.columnFormats = new LinkedHashMap<>();
    initializeDimensions(nonClusteringDimensions);

    final int clusteringCount = clusteringValues.length;
    this.groupTimePosition = timePosition < 0 ? -1 : Math.max(0, timePosition - clusteringCount);
    final int comparatorTimePosition = groupTimePosition < 0 ? dimensions.size() : groupTimePosition;

    final IncrementalIndex.IncrementalIndexRowComparator rowComparator =
        new IncrementalIndex.IncrementalIndexRowComparator(comparatorTimePosition, dimensions);
    if (rollup) {
      this.factsHolder = new OnheapIncrementalIndex.RollupFactsHolder(
          rowComparator,
          dimensions,
          comparatorTimePosition == 0
      );
    } else if (comparatorTimePosition == 0) {
      this.factsHolder = new OnheapIncrementalIndex.PlainTimeOrderedFactsHolder(rowComparator);
    } else {
      this.factsHolder = new OnheapIncrementalIndex.PlainNonTimeOrderedFactsHolder(rowComparator);
    }

    this.virtualColumns = virtualColumns;
    this.virtualSelectorFactory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
        IncrementalIndex.makeColumnSelectorFactory(virtualColumns, inputRowHolder, null)
    );
  }

  /**
   * The constant clustering tuple for this group, in clustering-column declaration order.
   */
  public Object[] getClusteringValues()
  {
    return clusteringValues;
  }

  /**
   * Per-clustering-column dictionary IDs assigned by the parent's segment-wide clustering indexers — these
   * become {@code TableClusterGroupSpec.clusteringValueIds} at persist time.
   */
  public List<Integer> getClusteringValueIds()
  {
    return clusteringValueIds;
  }

  @Override
  public List<IncrementalIndex.DimensionDesc> getDimensions()
  {
    return dimensions;
  }

  @Override
  public List<String> getDimensionNames(boolean includeTime)
  {
    if (!includeTime) {
      return ImmutableList.copyOf(dimensionsMap.keySet());
    }
    final ImmutableList.Builder<String> listBuilder = ImmutableList.builderWithExpectedSize(dimensionsMap.size() + 1);
    int i = 0;
    if (i == groupTimePosition) {
      listBuilder.add(ColumnHolder.TIME_COLUMN_NAME);
    }
    for (String dimName : dimensionsMap.keySet()) {
      listBuilder.add(dimName);
      i++;
      if (i == groupTimePosition) {
        listBuilder.add(ColumnHolder.TIME_COLUMN_NAME);
      }
    }
    return listBuilder.build();
  }

  @Override
  public List<String> getMetricNames()
  {
    return List.of();
  }

  @Override
  @Nullable
  public IncrementalIndex.DimensionDesc getDimension(String columnName)
  {
    return dimensionsMap.get(columnName);
  }

  @Override
  @Nullable
  public IncrementalIndex.MetricDesc getMetric(String columnName)
  {
    return null;
  }

  @Override
  @Nullable
  public ColumnFormat getColumnFormat(String columnName)
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
      return new CapabilitiesBasedFormat(ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG));
    }
    return columnFormats.get(columnName);
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    // Per-group ordering = segment ordering with the clustering-column prefix dropped (clustering values are
    // constants per group, so they don't participate in intra-group sorting).
    final List<OrderBy> segmentOrdering = parent.getSpec().getOrdering();
    final int clusteringCount = clusteringValues.length;
    if (segmentOrdering.size() <= clusteringCount) {
      return List.of();
    }
    return List.copyOf(segmentOrdering.subList(clusteringCount, segmentOrdering.size()));
  }

  @Override
  public int getTimePosition()
  {
    return groupTimePosition;
  }

  @Override
  public boolean isEmpty()
  {
    return numEntries.get() == 0;
  }

  @Override
  public int numRows()
  {
    return numEntries.get();
  }

  @Override
  public FactsHolder getFacts()
  {
    return factsHolder;
  }

  @Override
  public int getLastRowIndex()
  {
    // rowCounter is post-incremented only when a new row is added (see addToFacts), so it holds the count of rows;
    // the last assigned 0-based index is one less. Mirrors OnheapIncrementalIndex.getLastRowIndex(). Returning the
    // count instead would let a row appended after a cursor captured this value leak into that in-flight query.
    return rowCounter.get() - 1;
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    throw DruidException.defensive("clustered base table groups have no metrics");
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    throw DruidException.defensive("clustered base table groups have no metrics");
  }

  @Override
  public double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    throw DruidException.defensive("clustered base table groups have no metrics");
  }

  @Override
  @Nullable
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    throw DruidException.defensive("clustered base table groups have no metrics");
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    throw DruidException.defensive("clustered base table groups have no metrics");
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
      return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG).setHasNulls(false);
    }
    final IncrementalIndex.DimensionDesc dim = dimensionsMap.get(column);
    if (dim != null) {
      return dim.getCapabilities();
    }
    return null;
  }

  /**
   * Add a row's non-clustering content to this group's facts holder. Clustering values aren't re-processed here —
   * they've already been resolved + encoded by the parent {@link OnHeapClusteredBaseTable} as part of selecting
   * this group. Returns true when the row created a new fact entry (vs. rolling up into an existing one), so the
   * parent index can keep its segment-wide entry count in sync.
   */
  boolean addToFacts(
      InputRow row,
      long bucketedTimestamp,
      List<String> parseExceptionMessages,
      AtomicLong totalSizeInBytes
  )
  {
    final Object[] groupDims = new Object[dimensions.size()];
    long dimsKeySize = 0L;
    for (int i = 0; i < dimensions.size(); i++) {
      final IncrementalIndex.DimensionDesc desc = dimensions.get(i);
      final String name = desc.getName();
      // A column declared as a virtual-column output is computed through the (VC-aware) selector factory; a plain
      // column is read straight from the raw row.
      final Object dimValue = virtualColumns.exists(name)
                              ? virtualSelectorFactory.makeColumnValueSelector(name).getObject()
                              : row.getRaw(name);
      try {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final EncodedKeyComponent<?> k = ((DimensionIndexer) desc.getIndexer())
            .processRowValsToUnsortedEncodedKeyComponent(dimValue, true);
        groupDims[i] = k.getComponent();
        dimsKeySize += k.getEffectiveSizeBytes();
      }
      catch (ParseException pe) {
        parseExceptionMessages.add(pe.getMessage());
      }
    }
    totalSizeInBytes.addAndGet(dimsKeySize);

    final IncrementalIndexRow subKey = IncrementalIndexRow.createTimeAndDimswithDimsKeySize(
        bucketedTimestamp,
        groupDims,
        dimensions,
        dimsKeySize
    );

    final int priorIndex = factsHolder.getPriorIndex(subKey);
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      // with no metric columns there is nothing to aggregate
      return false;
    } else {
      final int rowIndex = rowCounter.getAndIncrement();
      final int prev = factsHolder.putIfAbsent(subKey, rowIndex);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        throw DruidException.defensive("Encountered existing fact entry for new key in cluster group");
      }
      final long rowSize = subKey.estimateBytesInMemory() + OnheapIncrementalIndex.ROUGH_OVERHEAD_PER_MAP_ENTRY;
      totalSizeInBytes.addAndGet(rowSize);
      return true;
    }
  }

  private void initializeDimensions(List<DimensionSchema> nonClusteringDimensions)
  {
    int i = 0;
    for (DimensionSchema schema : nonClusteringDimensions) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(schema.getName())) {
        // __time is handled via the row timestamp, not as a dim desc
        continue;
      }
      final IncrementalIndex.DimensionDesc desc = new IncrementalIndex.DimensionDesc(
          i++,
          schema.getName(),
          schema.getDimensionHandler()
      );
      dimensions.add(desc);
      dimensionsMap.put(schema.getName(), desc);
      columnFormats.put(schema.getName(), desc.getIndexer().getFormat());
    }
  }
}
