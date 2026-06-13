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
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One cluster group within an {@link OnHeapClusteredBaseTable}: a sub-IncrementalIndex holding the non-clustering
 * rows for a single clustering tuple. The clustering values themselves are NOT stored in this group's facts
 * holder — they're constants captured on {@link #clusteringValues} (per the share-nothing v1 design, clustering
 * values are written once on the on-disk group spec rather than per-row).
 * <p>
 * Each group owns its own {@link IncrementalIndex.DimensionDesc} list for the non-clustering dimensions, which
 * means own {@link org.apache.druid.segment.DimensionIndexer} instances and own dictionaries. That isolation is
 * what gives the persisted segment the "share nothing across groups" property — at persist time each group's
 * dictionaries are written under its per-group file-bundle prefix.
 * <p>
 * Aggregator state is also per-group: rollup (when enabled) deduplicates within a group's facts holder, never
 * across groups, since two rows with different clustering tuples land in different groups by construction.
 */
public final class OnHeapClusterGroup implements IncrementalIndexRowSelector
{
  private final OnHeapClusteredBaseTable parent;
  private final Object[] clusteringValues;
  private final List<Integer> clusteringValueIds;

  private final List<IncrementalIndex.DimensionDesc> dimensions;
  private final Map<String, IncrementalIndex.DimensionDesc> dimensionsMap;
  private final Map<String, IncrementalIndex.MetricDesc> aggregatorsMap;
  private final Map<String, ColumnFormat> columnFormats;
  private final AggregatorFactory[] aggregatorFactories;
  private final FactsHolder factsHolder;
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final AtomicInteger rowCounter = new AtomicInteger(0);
  private final AtomicInteger numEntries = new AtomicInteger(0);
  // Group-local time position within {@link #getOrdering()}: 0 for the canonical "[__time]" group ordering that
  // results from stripping the clustering-column prefix off the segment ordering.
  private final int groupTimePosition;

  private final ColumnSelectorFactory virtualSelectorFactory;
  private final Map<String, ColumnSelectorFactory> aggSelectors;

  OnHeapClusterGroup(
      OnHeapClusteredBaseTable parent,
      Object[] clusteringValues,
      List<Integer> clusteringValueIds,
      List<DimensionSchema> nonClusteringDimensions,
      AggregatorFactory[] aggregatorFactories,
      VirtualColumns virtualColumns,
      IncrementalIndex.InputRowHolder inputRowHolder,
      boolean rollup,
      int timePosition
  )
  {
    this.parent = parent;
    this.clusteringValues = clusteringValues;
    this.clusteringValueIds = Collections.unmodifiableList(new ArrayList<>(clusteringValueIds));
    this.aggregatorFactories = aggregatorFactories;

    this.dimensions = new ArrayList<>(nonClusteringDimensions.size());
    this.dimensionsMap = new LinkedHashMap<>();
    this.columnFormats = new LinkedHashMap<>();
    initializeDimensions(nonClusteringDimensions);

    // Group-local time position. {@code timePosition} arrives from the parent as the SEGMENT-level position of
    // __time within the segment's full ordering. Cluster groups strip the clustering-column prefix off that
    // ordering, so the per-group position is shifted by the size of the clustering tuple. For the canonical v1
    // ordering [clustering..., __time] this comes out to 0.
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

    this.virtualSelectorFactory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
        IncrementalIndex.makeColumnSelectorFactory(virtualColumns, inputRowHolder, null)
    );
    this.aggregatorsMap = new LinkedHashMap<>();
    this.aggSelectors = new LinkedHashMap<>();
    initializeAggregators(virtualColumns, inputRowHolder);
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
    return ImmutableList.copyOf(aggregatorsMap.keySet());
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
    return aggregatorsMap.get(columnName);
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
    return aggregators.get(rowOffset)[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return aggregators.get(rowOffset)[aggOffset].getLong();
  }

  @Override
  public double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return aggregators.get(rowOffset)[aggOffset].getDouble();
  }

  @Override
  @Nullable
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return aggregators.get(rowOffset)[aggOffset].get();
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    return aggregators.get(rowOffset)[aggOffset].isNull();
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
    final IncrementalIndex.MetricDesc metric = aggregatorsMap.get(column);
    if (metric != null) {
      return metric.getCapabilities();
    }
    return null;
  }

  public AggregatorFactory[] getAggregatorFactories()
  {
    return aggregatorFactories;
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
      try {
        @SuppressWarnings({"unchecked", "rawtypes"})
        final EncodedKeyComponent<?> k = ((org.apache.druid.segment.DimensionIndexer) desc.getIndexer())
            .processRowValsToUnsortedEncodedKeyComponent(row.getRaw(desc.getName()), true);
        groupDims[i] = k.getComponent();
        dimsKeySize += k.getEffectiveSizeBytes();
      }
      catch (org.apache.druid.java.util.common.parsers.ParseException pe) {
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
    final Aggregator[] aggs;
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggregators.get(priorIndex);
      final long aggSizeDelta = OnheapIncrementalIndex.doAggregate(
          aggregatorFactories,
          aggs,
          parent.getInputRowHolder(),
          parseExceptionMessages,
          false
      );
      totalSizeInBytes.addAndGet(aggSizeDelta);
      return false;
    } else {
      aggs = new Aggregator[aggregatorFactories.length];
      long aggSizeForRow = factorizeAggs(aggs);
      aggSizeForRow += OnheapIncrementalIndex.doAggregate(
          aggregatorFactories,
          aggs,
          parent.getInputRowHolder(),
          parseExceptionMessages,
          false
      );
      final int rowIndex = rowCounter.getAndIncrement();
      aggregators.put(rowIndex, aggs);
      final int prev = factsHolder.putIfAbsent(subKey, rowIndex);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        throw DruidException.defensive("Encountered existing fact entry for new key in cluster group");
      }
      final long rowSize = subKey.estimateBytesInMemory()
                           + aggSizeForRow
                           + OnheapIncrementalIndex.ROUGH_OVERHEAD_PER_MAP_ENTRY;
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

  private void initializeAggregators(
      VirtualColumns virtualColumns,
      IncrementalIndex.InputRowHolder inputRowHolder
  )
  {
    int i = 0;
    for (AggregatorFactory agg : aggregatorFactories) {
      final IncrementalIndex.MetricDesc metricDesc = new IncrementalIndex.MetricDesc(aggregatorsMap.size(), agg);
      aggregatorsMap.put(metricDesc.getName(), metricDesc);
      columnFormats.put(
          metricDesc.getName(),
          new org.apache.druid.segment.column.CapabilitiesBasedFormat(metricDesc.getCapabilities())
      );
      final ColumnSelectorFactory factory;
      if (agg.getIntermediateType().is(ValueType.COMPLEX)) {
        factory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
            IncrementalIndex.makeColumnSelectorFactory(VirtualColumns.EMPTY, inputRowHolder, agg)
        );
      } else {
        factory = virtualSelectorFactory;
      }
      aggSelectors.put(agg.getName(), factory);
      i++;
    }
  }

  private long factorizeAggs(Aggregator[] aggs)
  {
    long totalInitialSizeBytes = 0L;
    final long aggReferenceSize = Long.BYTES;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      final AggregatorFactory agg = aggregatorFactories[i];
      final AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(aggSelectors.get(agg.getName()));
      aggs[i] = aggregatorAndSize.getAggregator();
      totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
      totalInitialSizeBytes += aggReferenceSize;
    }
    return totalInitialSizeBytes;
  }
}
