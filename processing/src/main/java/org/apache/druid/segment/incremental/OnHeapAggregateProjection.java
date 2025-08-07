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
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAndSize;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

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
  private final long minTimestamp;
  private final AtomicInteger rowCounter = new AtomicInteger(0);
  private final AtomicInteger numEntries = new AtomicInteger(0);
  @Nullable
  private final ValueMatcher valueMatcher;

  public OnHeapAggregateProjection(
      AggregateProjectionSpec projectionSpec,
      Function<String, IncrementalIndex.DimensionDesc> getBaseTableDimensionDesc,
      Function<String, AggregatorFactory> getBaseTableAggregatorFactory,
      long minTimestamp
  )
  {
    this.projectionSchema = projectionSpec.toMetadataSchema();
    this.minTimestamp = minTimestamp;

    // initialize dimensions, facts holder
    this.dimensions = new ArrayList<>();
    // mapping of position in descs on the projection to position in the parent incremental index. Like the parent
    // incremental index, the time (or time-like) column does not have a dimension descriptor and is specially
    // handled as the timestamp of the row. Unlike the parent incremental index, an aggregating projection will
    // always have its time-like column in the grouping columns list, so its position in this array specifies -1
    this.parentDimensionIndex = new int[projectionSpec.getGroupingColumns().size()];
    Arrays.fill(parentDimensionIndex, -1);
    this.dimensionsMap = new HashMap<>();
    this.columnFormats = new LinkedHashMap<>();

    initializeAndValidateDimensions(projectionSpec, getBaseTableDimensionDesc);
    final IncrementalIndex.IncrementalIndexRowComparator rowComparator = new IncrementalIndex.IncrementalIndexRowComparator(
        projectionSchema.getTimeColumnPosition() < 0 ? dimensions.size() : projectionSchema.getTimeColumnPosition(),
        dimensions
    );
    this.factsHolder = new OnheapIncrementalIndex.RollupFactsHolder(
        rowComparator,
        dimensions,
        projectionSchema.getTimeColumnPosition() == 0
    );

    // validate virtual columns refer to base table dimensions and initialize selector factory
    validateVirtualColumns(projectionSpec, getBaseTableDimensionDesc);
    this.virtualSelectorFactory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
        IncrementalIndex.makeColumnSelectorFactory(projectionSchema.getVirtualColumns(), inputRowHolder, null)
    );
    // initialize aggregators
    this.aggSelectors = new LinkedHashMap<>();
    this.aggregatorsMap = new LinkedHashMap<>();
    this.aggregatorFactories = new AggregatorFactory[projectionSchema.getAggregators().length];
    initializeAndValidateAggregators(projectionSpec, getBaseTableDimensionDesc, getBaseTableAggregatorFactory);

    if (projectionSpec.getFilter() != null) {
      RowSignature.Builder bob = RowSignature.builder();
      if (projectionSchema.getTimeColumnPosition() < 0) {
        bob.addTimeColumn();
      }
      for (String groupingColumn : projectionSchema.getGroupingColumns()) {
        if (projectionSchema.getTimeColumnName().equals(groupingColumn)) {
          bob.addTimeColumn();
        } else {
          bob.add(groupingColumn, dimensionsMap.get(groupingColumn).getCapabilities().toColumnType());
        }
      }
      valueMatcher = projectionSchema.getFilter()
                                     .toFilter()
                                     .makeMatcher(
                                         RowBasedColumnSelectorFactory.create(
                                             RowAdapters.standardRow(),
                                             inputRowHolder::getRow,
                                             bob.build(),
                                             false,
                                             false
                                         )
                                     );
    } else {
      valueMatcher = null;
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
    if (valueMatcher != null && !valueMatcher.matches(false)) {
      return;
    }
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
    final long timestamp;

    if (projectionSchema.getTimeColumnName() != null) {
      timestamp = projectionSchema.getEffectiveGranularity().bucketStart(DateTimes.utc(key.getTimestamp())).getMillis();
      if (timestamp < minTimestamp) {
        throw DruidException.defensive(
            "Cannot add row[%s] to projection[%s] because projection effective timestamp[%s] is below the minTimestamp[%s]",
            inputRow,
            projectionSchema.getName(),
            DateTimes.utc(timestamp),
            DateTimes.utc(minTimestamp)
        );
      }
    } else {
      timestamp = minTimestamp;
    }
    final IncrementalIndexRow subKey = new IncrementalIndexRow(
        timestamp,
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
          false
      );
      totalSizeInBytes.addAndGet(aggForProjectionSizeDelta);
    } else {
      aggs = new Aggregator[aggregatorFactories.length];
      long aggSizeForProjectionRow = factorizeAggs(aggregatorFactories, aggs);
      aggSizeForProjectionRow += OnheapIncrementalIndex.doAggregate(
          aggregatorFactories,
          aggs,
          inputRowHolder,
          parseExceptionMessages,
          false
      );
      final long estimatedSizeOfAggregators = aggSizeForProjectionRow;
      final long projectionRowSize = key.estimateBytesInMemory()
                                     + estimatedSizeOfAggregators
                                     + OnheapIncrementalIndex.ROUGH_OVERHEAD_PER_MAP_ENTRY;
      totalSizeInBytes.addAndGet(projectionRowSize);
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

  private void validateVirtualColumns(
      AggregateProjectionSpec projectionSpec,
      Function<String, IncrementalIndex.DimensionDesc> getBaseTableDimensionDesc
  )
  {
    for (VirtualColumn vc : projectionSchema.getVirtualColumns().getVirtualColumns()) {
      for (String column : vc.requiredColumns()) {
        if (column.equals(projectionSchema.getTimeColumnName()) || column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
          continue;
        }
        if (getBaseTableDimensionDesc.apply(column) == null) {
          throw InvalidInput.exception(
              "projection[%s] contains virtual column[%s] that references an input[%s] which is not a dimension in the base table",
              projectionSpec.getName(),
              vc.getOutputName(),
              column
          );
        }
      }
    }
  }

  private void initializeAndValidateDimensions(
      AggregateProjectionSpec projectionSpec,
      Function<String, IncrementalIndex.DimensionDesc> getBaseTableDimensionDesc
  )
  {
    int i = 0;
    for (DimensionSchema dimension : projectionSpec.getGroupingColumns()) {
      if (dimension.getName().equals(projectionSchema.getTimeColumnName())) {
        columnFormats.put(
            dimension.getName(),
            new CapabilitiesBasedFormat(ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG))
        );
        continue;
      }
      final IncrementalIndex.DimensionDesc parent = getBaseTableDimensionDesc.apply(dimension.getName());
      if (parent == null) {
        // ensure that this dimension refers to a virtual column, otherwise it is invalid
        if (!projectionSpec.getVirtualColumns().exists(dimension.getName())) {
          throw InvalidInput.exception(
              "projection[%s] contains dimension[%s] that is not present on the base table or a virtual column",
              projectionSpec.getName(),
              dimension.getName()
          );
        }
        // this dimension only exists in the child, it needs its own handler
        final IncrementalIndex.DimensionDesc childOnly = new IncrementalIndex.DimensionDesc(
            i++,
            dimension.getName(),
            dimension.getDimensionHandler()
        );

        dimensions.add(childOnly);
        dimensionsMap.put(dimension.getName(), childOnly);
        columnFormats.put(dimension.getName(), childOnly.getIndexer().getFormat());
      } else {
        if (!dimension.getColumnType().equals(parent.getCapabilities().toColumnType())) {
          // special handle auto column schema, who reports type as json in schema, but indexer reports whatever
          // type it has seen, which is string at this stage
          boolean allowAuto = ColumnType.NESTED_DATA.equals(dimension.getColumnType()) &&
                              parent.getIndexer() instanceof AutoTypeColumnIndexer;
          InvalidInput.conditionalException(
              allowAuto,
              "projection[%s] contains dimension[%s] with different type[%s] than type[%s] in base table",
              projectionSpec.getName(),
              dimension.getName(),
              dimension.getColumnType(),
              parent.getCapabilities().toColumnType()
          );
        }
        // make a new DimensionDesc from the child, containing all of the parents stuff but with the childs position
        final IncrementalIndex.DimensionDesc child = new IncrementalIndex.DimensionDesc(
            i++,
            parent.getName(),
            parent.getHandler(),
            parent.getIndexer()
        );

        dimensions.add(child);
        dimensionsMap.put(dimension.getName(), child);
        parentDimensionIndex[child.getIndex()] = parent.getIndex();
        columnFormats.put(dimension.getName(), child.getIndexer().getFormat());
      }
    }
  }

  private void initializeAndValidateAggregators(
      AggregateProjectionSpec projectionSpec,
      Function<String, IncrementalIndex.DimensionDesc> getBaseTableDimensionDesc,
      Function<String, AggregatorFactory> getBaseTableAggregatorFactory
  )
  {
    int i = 0;
    for (AggregatorFactory agg : projectionSchema.getAggregators()) {
      AggregatorFactory aggToUse = agg;
      AggregatorFactory baseTableAgg = getBaseTableAggregatorFactory.apply(agg.getName());
      if (baseTableAgg != null) {
        // if the aggregator references a base table aggregator, it must have the same name and be a combining aggregator
        // of the base table agg
        if (!agg.equals(baseTableAgg.getCombiningFactory())) {
          throw InvalidInput.exception(
              "projection[%s] contains aggregator[%s] that is not the 'combining' aggregator of base table aggregator[%s]",
              projectionSpec.getName(),
              agg.getName(),
              agg.getName()
          );
        }
        aggToUse = baseTableAgg;
      } else {
        // otherwise, the aggregator must reference base table dimensions
        for (String column : agg.requiredFields()) {
          if (column.equals(projectionSchema.getTimeColumnName()) || column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
            continue;
          }
          if (getBaseTableAggregatorFactory.apply(column) != null) {
            throw InvalidInput.exception(
                "projection[%s] contains aggregator[%s] that references aggregator[%s] in base table but this is not supported, projection aggregators which reference base table aggregates must be 'combining' aggregators with the same name as the base table column",
                projectionSpec.getName(),
                agg.getName(),
                column
            );
          }
          if (getBaseTableDimensionDesc.apply(column) == null) {
            // aggregators with virtual column inputs are not supported yet. Supporting this requires some additional
            // work so that there is a way to do something like rename aggregators input column names so the projection
            // agg which references the projection virtual column can be changed to the query virtual column name
            // (since the query agg references the query virtual column). Disallow but provide a helpful error for now
            if (projectionSchema.getVirtualColumns().exists(column)) {
              throw InvalidInput.exception(
                  "projection[%s] contains aggregator[%s] that is has required field[%s] which is a virtual column, this is not yet supported",
                  projectionSpec.getName(),
                  agg.getName(),
                  column
              );
            }
            // not a virtual column, doesn't refer to a base table dimension either, bail instead of ingesting a bunch
            // of nulls
            throw InvalidInput.exception(
                "projection[%s] contains aggregator[%s] that is missing required field[%s] in base table",
                projectionSpec.getName(),
                agg.getName(),
                column
            );
          }
        }
      }
      IncrementalIndex.MetricDesc metricDesc = new IncrementalIndex.MetricDesc(aggregatorsMap.size(), aggToUse);
      aggregatorsMap.put(metricDesc.getName(), metricDesc);
      columnFormats.put(metricDesc.getName(), new CapabilitiesBasedFormat(metricDesc.getCapabilities()));
      final ColumnSelectorFactory factory;
      if (agg.getIntermediateType().is(ValueType.COMPLEX)) {
        factory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
            IncrementalIndex.makeColumnSelectorFactory(VirtualColumns.EMPTY, inputRowHolder, aggToUse)
        );
      } else {
        factory = virtualSelectorFactory;
      }
      aggSelectors.put(aggToUse.getName(), factory);
      aggregatorFactories[i++] = aggToUse;
    }
  }

  private long factorizeAggs(AggregatorFactory[] aggregatorFactories, Aggregator[] aggs)
  {
    long totalInitialSizeBytes = 0L;
    final long aggReferenceSize = Long.BYTES;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      final AggregatorFactory agg = aggregatorFactories[i];
      // Creates aggregators to aggregate from input into output fields
      AggregatorAndSize aggregatorAndSize = agg.factorizeWithSize(aggSelectors.get(agg.getName()));
      aggs[i] = aggregatorAndSize.getAggregator();
      totalInitialSizeBytes += aggregatorAndSize.getInitialSizeBytes();
      totalInitialSizeBytes += aggReferenceSize;
    }
    return totalInitialSizeBytes;
  }
}
