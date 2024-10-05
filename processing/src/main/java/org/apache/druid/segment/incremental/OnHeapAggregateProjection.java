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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Projection of {@link OnheapIncrementalIndex} for {@link org.apache.druid.data.input.impl.AggregateProjectionSpec}
 */
public class OnHeapAggregateProjection implements IncrementalIndexRowSelector
{
  private final AggregateProjectionMetadata.Schema projectionSchema;
  private final List<IncrementalIndex.DimensionDesc> dimensions;
  private final int[] parentDimensionIndex;
  private final AggregatorFactory[] aggregatorFactories;
  private final Map<String, IncrementalIndex.DimensionDesc> dimensionsMap;
  private final Map<String, IncrementalIndex.MetricDesc> aggregatorsMap;
  private final Map<String, ColumnFormat> columnFormats;
  private final FactsHolder factsHolder;
  private final IncrementalIndex.InputRowHolder inputRowHolder = new IncrementalIndex.InputRowHolder();
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final ColumnSelectorFactory virtualSelectorFactory;
  private final Map<String, ColumnSelectorFactory> aggSelectors;
  private final boolean useMaxMemoryEstimates;
  private final long maxBytesPerRowForAggregators;
  private final long minTimestamp;
  private final AtomicInteger rowCounter = new AtomicInteger(0);
  private final AtomicInteger numEntries = new AtomicInteger(0);

  public OnHeapAggregateProjection(
      AggregateProjectionMetadata.Schema schema,
      List<IncrementalIndex.DimensionDesc> dimensions,
      Map<String, IncrementalIndex.DimensionDesc> dimensionsMap,
      int[] parentDimensionIndex,
      long minTimestamp,
      boolean useMaxMemoryEstimates,
      long maxBytesPerRowForAggregators
  )
  {
    this.projectionSchema = schema;
    this.dimensions = dimensions;
    this.parentDimensionIndex = parentDimensionIndex;
    this.dimensionsMap = dimensionsMap;
    this.minTimestamp = minTimestamp;
    final IncrementalIndex.IncrementalIndexRowComparator rowComparator = new IncrementalIndex.IncrementalIndexRowComparator(
        projectionSchema.getTimeColumnPosition() < 0 ? dimensions.size() : projectionSchema.getTimeColumnPosition(),
        dimensions
    );
    this.factsHolder = new OnheapIncrementalIndex.RollupFactsHolder(
        rowComparator,
        dimensions,
        projectionSchema.getTimeColumnPosition() == 0
    );
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    this.maxBytesPerRowForAggregators = maxBytesPerRowForAggregators;

    this.virtualSelectorFactory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
        IncrementalIndex.makeColumnSelectorFactory(schema.getVirtualColumns(), inputRowHolder, null)
    );
    this.aggSelectors = new LinkedHashMap<>();
    this.aggregatorsMap = new LinkedHashMap<>();
    this.aggregatorFactories = new AggregatorFactory[schema.getAggregators().length];
    this.columnFormats = new LinkedHashMap<>();
    for (IncrementalIndex.DimensionDesc dimension : dimensions) {
      if (dimension.getName().equals(projectionSchema.getTimeColumnName())) {
        columnFormats.put(
            dimension.getName(),
            new CapabilitiesBasedFormat(ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG))
        );
      } else {
        columnFormats.put(dimension.getName(), dimension.getIndexer().getFormat());
      }
    }
    int i = 0;
    for (AggregatorFactory agg : schema.getAggregators()) {
      IncrementalIndex.MetricDesc metricDesc = new IncrementalIndex.MetricDesc(aggregatorsMap.size(), agg);
      aggregatorsMap.put(metricDesc.getName(), metricDesc);
      columnFormats.put(metricDesc.getName(), new CapabilitiesBasedFormat(metricDesc.getCapabilities()));
      final ColumnSelectorFactory factory;
      if (agg.getIntermediateType().is(ValueType.COMPLEX)) {
        factory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
            IncrementalIndex.makeColumnSelectorFactory(VirtualColumns.EMPTY, inputRowHolder, agg)
        );
      } else {
        factory = virtualSelectorFactory;
      }
      aggSelectors.put(agg.getName(), factory);
      aggregatorFactories[i++] = agg;
    }
  }

  /**
   * Add row to projection {@link #factsHolder}, updating totalSizeInBytes estimate
   */
  public void addToFacts(
      IncrementalIndexRow key,
      InputRow inputRow,
      List<String> parseExceptionMessages,
      AtomicLong totalSizeInBytes
  )
  {
    inputRowHolder.set(inputRow);
    final Object[] projectionDims = new Object[dimensions.size()];
    for (int i = 0; i < projectionDims.length; i++) {
      int parentDimIndex = parentDimensionIndex[i];
      if (parentDimIndex < 0) {
        IncrementalIndex.DimensionDesc desc = dimensions.get(i);
        final ColumnValueSelector<?> virtualSelector = virtualSelectorFactory.makeColumnValueSelector(desc.getName());
        EncodedKeyComponent<?> k = desc.getIndexer().processRowValsToUnsortedEncodedKeyComponent(
            virtualSelector.getObject(),
            false
        );
        projectionDims[i] = k.getComponent();
        totalSizeInBytes.addAndGet(k.getEffectiveSizeBytes());
      } else {
        projectionDims[i] = key.dims[parentDimensionIndex[i]];
      }
    }
    final IncrementalIndexRow subKey = new IncrementalIndexRow(
        projectionSchema.getTimeColumnName() != null
        ? projectionSchema.getGranularity().bucketStart(DateTimes.utc(key.getTimestamp())).getMillis()
        : minTimestamp,
        projectionDims,
        dimensions
    );

    final int priorIndex = factsHolder.getPriorIndex(subKey);

    final Aggregator[] aggs;
    if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggregators.get(priorIndex);
      long aggForProjectionSizeDelta = OnheapIncrementalIndex.doAggregate(
          aggregatorFactories,
          aggs,
          inputRowHolder,
          parseExceptionMessages,
          useMaxMemoryEstimates,
          false
      );
      totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : aggForProjectionSizeDelta);
    } else {
      aggs = new Aggregator[aggregatorFactories.length];
      long aggSizeForProjectionRow = factorizeAggs(aggregatorFactories, aggs);
      aggSizeForProjectionRow += OnheapIncrementalIndex.doAggregate(
          aggregatorFactories,
          aggs,
          inputRowHolder,
          parseExceptionMessages,
          useMaxMemoryEstimates,
          false
      );
      final long estimatedSizeOfAggregators =
          useMaxMemoryEstimates ? maxBytesPerRowForAggregators : aggSizeForProjectionRow;
      final long projectionRowSize = key.estimateBytesInMemory()
                                     + estimatedSizeOfAggregators
                                     + OnheapIncrementalIndex.ROUGH_OVERHEAD_PER_MAP_ENTRY;
      totalSizeInBytes.addAndGet(useMaxMemoryEstimates ? 0 : projectionRowSize);
      numEntries.incrementAndGet();
    }
    final int rowIndex = rowCounter.getAndIncrement();
    aggregators.put(rowIndex, aggs);
    factsHolder.putIfAbsent(subKey, rowIndex);
  }

  @Override
  public FactsHolder getFacts()
  {
    return factsHolder;
  }

  @Override
  public List<IncrementalIndex.DimensionDesc> getDimensions()
  {
    return dimensions;
  }

  @Override
  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(aggregatorsMap.keySet());
  }

  @Override
  public IncrementalIndex.DimensionDesc getDimension(String columnName)
  {
    return dimensionsMap.get(columnName);
  }

  @Override
  public IncrementalIndex.MetricDesc getMetric(String columnName)
  {
    return aggregatorsMap.get(columnName);
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    // return ordering with projection time column substituted with __time so query engines can treat it equivalently
    return projectionSchema.getOrderingWithTimeColumnSubstitution();
  }

  @Override
  public int getTimePosition()
  {
    return projectionSchema.getTimeColumnPosition();
  }

  @Override
  public boolean isEmpty()
  {
    return rowCounter.get() == 0;
  }

  @Override
  public int getLastRowIndex()
  {
    return rowCounter.get();
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

  @Nullable
  @Override
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
  public ColumnFormat getColumnFormat(String columnName)
  {
    return columnFormats.get(columnName);
  }

  @Override
  public int numRows()
  {
    return numEntries.get();
  }

  @Override
  public List<String> getDimensionNames(boolean includeTime)
  {
    synchronized (dimensionsMap) {
      if (includeTime && projectionSchema.getTimeColumnName() != null) {
        final ImmutableList.Builder<String> listBuilder =
            ImmutableList.builderWithExpectedSize(dimensionsMap.size() + 1);
        int i = 0;
        if (i == projectionSchema.getTimeColumnPosition()) {
          listBuilder.add(projectionSchema.getTimeColumnName());
        }
        for (String dimName : dimensionsMap.keySet()) {
          listBuilder.add(dimName);
          i++;
          if (i == projectionSchema.getTimeColumnPosition()) {
            listBuilder.add(projectionSchema.getTimeColumnName());
          }
        }
        return listBuilder.build();
      } else {
        return ImmutableList.copyOf(dimensionsMap.keySet());
      }
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column) || Objects.equals(column, projectionSchema.getTimeColumnName())) {
      return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG).setHasNulls(false);
    }
    if (dimensionsMap.containsKey(column)) {
      return dimensionsMap.get(column).getCapabilities();
    }
    if (aggregatorsMap.containsKey(column)) {
      return aggregatorsMap.get(column).getCapabilities();
    }
    return null;
  }

  public Map<String, IncrementalIndex.DimensionDesc> getDimensionsMap()
  {
    return dimensionsMap;
  }

  public AggregateProjectionMetadata toMetadata()
  {
    return new AggregateProjectionMetadata(projectionSchema, numEntries.get());
  }

  private long factorizeAggs(AggregatorFactory[] aggregatorFactories, Aggregator[] aggs)
  {
    long totalInitialSizeBytes = 0L;
    final long aggReferenceSize = Long.BYTES;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      final AggregatorFactory agg = aggregatorFactories[i];
      // Creates aggregators to aggregate from input into output fields
      if (useMaxMemoryEstimates) {
        aggs[i] = agg.factorize(aggSelectors.get(agg.getName()));
      } else {
        AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(aggSelectors.get(agg.getName()));
        aggs[i] = aggregatorAndSize.getAggregator();
        totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
        totalInitialSizeBytes += aggReferenceSize;
      }
    }
    return totalInitialSizeBytes;
  }
}
