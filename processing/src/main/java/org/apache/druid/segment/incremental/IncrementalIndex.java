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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.SpatialDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.UnparseableColumnsParseException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.NestedCommonFormatColumnHandler;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.CapabilitiesBasedFormat;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.transform.TransformedInputRow;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * In-memory, row-based data structure used to hold data during ingestion. Realtime tasks query this index using
 * {@link IncrementalIndexCursorFactory}.
 *
 * Concurrency model: {@link #add(InputRow)} is not thread-safe, and must be called from a single thread or externally
 * synchronized. However, the methods that support {@link IncrementalIndexCursorFactory} are thread-safe, and may be
 * called concurrently with each other, and with the "add" methods. This concurrency model supports real-time queries
 * of the data in the index.
 */
public abstract class IncrementalIndex implements IncrementalIndexRowSelector, ColumnInspector, Iterable<Row>, Closeable
{
  /**
   * Column selector used at ingestion time for inputs to aggregators.
   *
   * @param virtualColumns virtual columns
   * @param inputRowHolder ingestion-time input row holder
   * @param agg            the aggregator, or null to make a generic aggregator. Only required if the agg has
   *                       {@link AggregatorFactory#getIntermediateType()} as {@link ValueType#COMPLEX}, because
   *                       in this case we need to do some magic to ensure the correct values show up.
   *
   * @return column selector factory
   */
  public static ColumnSelectorFactory makeColumnSelectorFactory(
      final VirtualColumns virtualColumns,
      final InputRowHolder inputRowHolder,
      @Nullable final AggregatorFactory agg
  )
  {
    // we use RowSignature.empty() because ColumnInspector here should be the InputRow schema, not the
    // IncrementalIndex schema, because we are reading values from the InputRow
    final RowBasedColumnSelectorFactory<InputRow> baseSelectorFactory = new RowBasedColumnSelectorFactory<>(
        inputRowHolder::getRow,
        inputRowHolder::getRowId,
        RowAdapters.standardRow(),
        RowSignature.empty(),
        true,
        true
    );

    class IncrementalIndexInputRowColumnSelectorFactory implements ColumnSelectorFactory
    {
      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(final String column)
      {
        final ColumnValueSelector selector = baseSelectorFactory.makeColumnValueSelector(column);

        if (agg == null || !agg.getIntermediateType().is(ValueType.COMPLEX)) {
          return selector;
        } else {
          // Wrap selector in a special one that uses ComplexMetricSerde to modify incoming objects.
          // For complex aggregators that read from multiple columns, we wrap all of them. This is not ideal but it
          // has worked so far.
          final String complexTypeName = agg.getIntermediateType().getComplexTypeName();
          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(complexTypeName);
          if (serde == null) {
            throw new ISE("Don't know how to handle type[%s]", complexTypeName);
          }

          final ComplexMetricExtractor extractor = serde.getExtractor();
          return new ColumnValueSelector()
          {
            @Override
            public boolean isNull()
            {
              return selector.isNull();
            }

            @Override
            public long getLong()
            {
              return selector.getLong();
            }

            @Override
            public float getFloat()
            {
              return selector.getFloat();
            }

            @Override
            public double getDouble()
            {
              return selector.getDouble();
            }

            @Override
            public Class classOfObject()
            {
              return extractor.extractedClass();
            }

            @Nullable
            @Override
            public Object getObject()
            {
              // Here is where the magic happens: read from "in" directly, don't go through the normal "selector".
              return extractor.extractValue(inputRowHolder.getRow(), column, agg);
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("inputRowHolder", inputRowHolder);
              inspector.visit("selector", selector);
              inspector.visit("extractor", extractor);
            }
          };
        }
      }

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return baseSelectorFactory.makeDimensionSelector(dimensionSpec);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        return baseSelectorFactory.getColumnCapabilities(columnName);
      }
    }

    return virtualColumns.wrap(new IncrementalIndexInputRowColumnSelectorFactory());
  }

  private final long minTimestamp;
  private final Granularity queryGranularity;
  private final boolean rollup;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final VirtualColumns virtualColumns;
  private final AggregatorFactory[] metrics;
  /**
   * Metadata to be persisted along with this index, when it is eventually persisted.
   */
  private final Metadata metadata;
  protected final boolean preserveExistingMetrics;

  private final Map<String, MetricDesc> metricDescs;

  private final DimensionsSpec dimensionsSpec;
  private final Map<String, DimensionDesc> dimensionDescs;
  /**
   * Position of {@link ColumnHolder#TIME_COLUMN_NAME} in the sort order, relative to elements of
   * {@link #dimensionDescs}. For example, for the sort order [x, __time, y], dimensionDescs contains [x, y] and
   * timePosition is 1.
   */
  protected final int timePosition;
  private final List<DimensionDesc> dimensionDescsList;
  // dimension capabilities are provided by the indexers
  private final Map<String, ColumnCapabilities> timeAndMetricsColumnCapabilities;
  private final Map<String, ColumnFormat> timeAndMetricsColumnFormats;
  private final AtomicInteger numEntries = new AtomicInteger();
  private final AtomicLong bytesInMemory = new AtomicLong();

  private final boolean useSchemaDiscovery;

  protected final InputRowHolder inputRowHolder = new InputRowHolder();

  @Nullable
  private volatile DateTime maxIngestedEventTime;

  /**
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param preserveExistingMetrics   When set to true, for any row that already has metric
   *                                  (with the same name defined in metricSpec), the metric aggregator in metricSpec
   *                                  is skipped and the existing metric is unchanged. If the row does not already have
   *                                  the metric, then the metric aggregator is applied on the source column as usual.
   *                                  This should only be set for DruidInputSource since that is the only case where we
   *                                  can have existing metrics. This is currently only use by auto compaction and
   *                                  should not be use for anything else.
   */
  protected IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean preserveExistingMetrics
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.queryGranularity = incrementalIndexSchema.getQueryGranularity();
    this.rollup = incrementalIndexSchema.isRollup();
    this.virtualColumns = incrementalIndexSchema.getVirtualColumns();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.preserveExistingMetrics = preserveExistingMetrics;
    this.useSchemaDiscovery = incrementalIndexSchema.getDimensionsSpec()
                                                    .useSchemaDiscovery();

    this.timeAndMetricsColumnCapabilities = new HashMap<>();
    this.timeAndMetricsColumnFormats = new HashMap<>();
    this.metricDescs = Maps.newLinkedHashMap();
    this.dimensionDescs = Maps.newLinkedHashMap();

    initAggs(metrics, inputRowHolder);

    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      final ColumnCapabilities capabilities = metricDesc.getCapabilities();
      timeAndMetricsColumnCapabilities.put(metricDesc.getName(), capabilities);
      if (capabilities.is(ValueType.COMPLEX)) {
        timeAndMetricsColumnFormats.put(
            metricDesc.getName(),
            new CapabilitiesBasedFormat(
                ColumnCapabilitiesImpl.snapshot(
                    ColumnCapabilitiesImpl.copyOf(capabilities).setType(ColumnType.ofComplex(metricDesc.getType())),
                    ColumnCapabilitiesImpl.ALL_FALSE
                )
            )
        );
      } else {
        timeAndMetricsColumnFormats.put(
            metricDesc.getName(),
            new CapabilitiesBasedFormat(
                ColumnCapabilitiesImpl.snapshot(capabilities, ColumnCapabilitiesImpl.ALL_FALSE)
            )
        );
      }

    }

    this.dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();

    this.dimensionDescsList = new ArrayList<>();

    int foundTimePosition = -1;
    final List<DimensionSchema> dimSchemas = dimensionsSpec.getDimensions();
    for (int i = 0; i < dimSchemas.size(); i++) {
      final DimensionSchema dimSchema = dimSchemas.get(i);
      if (ColumnHolder.TIME_COLUMN_NAME.equals(dimSchema.getName())) {
        foundTimePosition = i;
      } else {
        addNewDimension(dimSchema.getName(), dimSchema.getDimensionHandler());
      }
    }

    if (foundTimePosition == -1) {
      // __time not found: that means it either goes at the end, or the beginning, based on
      // forceSegmentSortByTime.
      this.timePosition = dimensionsSpec.isForceSegmentSortByTime() ? 0 : dimensionDescsList.size();
    } else {
      this.timePosition = foundTimePosition;
    }

    //__time capabilities
    timeAndMetricsColumnCapabilities.put(
        ColumnHolder.TIME_COLUMN_NAME,
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
    );

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = dimensionsSpec.getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }

    // Set metadata last, so dimensionOrder is populated
    this.metadata = new Metadata(
        null,
        getCombiningAggregators(metrics),
        incrementalIndexSchema.getTimestampSpec(),
        this.queryGranularity,
        this.rollup,
        getDimensionOrder().stream().map(OrderBy::ascending).collect(Collectors.toList()),
        Collections.emptyList()
    );
  }

  @Nullable
  public abstract QueryableProjection<IncrementalIndexRowSelector> getProjection(CursorBuildSpec buildSpec);

  public abstract IncrementalIndexRowSelector getProjection(String name);

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract void initAggs(
      AggregatorFactory[] metrics,
      InputRowHolder rowSupplier
  );

  // Note: This method does not need to be thread safe.
  protected abstract AddToFactsResult addToFacts(
      IncrementalIndexRow key,
      InputRowHolder inputRowHolder
  );


  public abstract Iterable<Row> iterableWithPostAggregations(
      @Nullable List<PostAggregator> postAggs,
      boolean descending
  );

  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  public void close()
  {
  }

  public InputRow formatRow(InputRow row)
  {
    for (Function<InputRow, InputRow> rowTransformer : rowTransformers) {
      row = rowTransformer.apply(row);
    }

    if (row == null) {
      throw new IAE("Row is null? How can this be?!");
    }
    return row;
  }

  public Map<String, ColumnFormat> getColumnFormats()
  {
    ImmutableMap.Builder<String, ColumnFormat> builder = ImmutableMap.builder();

    synchronized (dimensionDescs) {
      timeAndMetricsColumnFormats.forEach(builder::put);
      dimensionDescs.forEach((dimension, desc) -> builder.put(dimension, desc.getIndexer().getFormat()));
    }
    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (timeAndMetricsColumnCapabilities.containsKey(columnName)) {
      return timeAndMetricsColumnCapabilities.get(columnName);
    }
    synchronized (dimensionDescs) {
      final DimensionDesc desc = dimensionDescs.get(columnName);
      return desc != null ? desc.getCapabilities() : null;
    }
  }

  @Nullable
  @Override
  public ColumnFormat getColumnFormat(String columnName)
  {
    if (timeAndMetricsColumnFormats.containsKey(columnName)) {
      return timeAndMetricsColumnFormats.get(columnName);
    }

    synchronized (dimensionDescs) {
      final DimensionDesc desc = dimensionDescs.get(columnName);
      return desc != null ? desc.getIndexer().getFormat() : null;
    }
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   *
   * Not thread-safe.
   *
   * @param row                      the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow. If any parse failure occurs, a {@link ParseException} is returned in {@link IncrementalIndexAddResult}.
   */
  public IncrementalIndexAddResult add(InputRow row)
  {
    IncrementalIndexRowResult incrementalIndexRowResult = toIncrementalIndexRow(row);
    inputRowHolder.set(row);
    final AddToFactsResult addToFactsResult = addToFacts(
        incrementalIndexRowResult.getIncrementalIndexRow(),
        inputRowHolder
    );
    updateMaxIngestedTime(row.getTimestamp());
    @Nullable ParseException parseException = getCombinedParseException(
        row,
        incrementalIndexRowResult.getParseExceptionMessages(),
        addToFactsResult.getParseExceptionMessages()
    );
    inputRowHolder.unset();
    return new IncrementalIndexAddResult(
        addToFactsResult.getRowCount(),
        addToFactsResult.getBytesInMemory(),
        parseException
    );
  }

  @VisibleForTesting
  IncrementalIndexRowResult toIncrementalIndexRow(InputRow row)
  {
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, DateTimes.utc(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();
    Object[] dims;
    List<Object> overflow = null;
    long dimsKeySize = 0;
    List<String> parseExceptionMessages = new ArrayList<>();
    synchronized (dimensionDescs) {
      // all known dimensions are assumed missing until we encounter in the rowDimensions
      Set<String> absentDimensions = Sets.newHashSet(dimensionDescs.keySet());

      // first, process dimension values present in the row
      dims = new Object[dimensionDescs.size()];
      for (String dimension : rowDimensions) {
        if (Strings.isNullOrEmpty(dimension) || ColumnHolder.TIME_COLUMN_NAME.equals(dimension)) {
          continue;
        }
        boolean wasNewDim = false;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          absentDimensions.remove(dimension);
        } else {
          wasNewDim = true;
          final DimensionHandler<?, ?, ?> handler;
          if (useSchemaDiscovery) {
            handler = new NestedCommonFormatColumnHandler(dimension, null);
          } else {
            // legacy behavior: for schemaless type discovery, everything is a String
            handler = DimensionHandlerUtils.getHandlerFromCapabilities(
                dimension,
                makeDefaultCapabilitiesFromValueType(ColumnType.STRING),
                null
            );
          }
          desc = addNewDimension(dimension, handler);
        }
        DimensionIndexer indexer = desc.getIndexer();
        Object dimsKey = null;
        try {
          final EncodedKeyComponent<?> encodedKeyComponent
              = indexer.processRowValsToUnsortedEncodedKeyComponent(row.getRaw(dimension), true);
          dimsKey = encodedKeyComponent.getComponent();
          dimsKeySize += encodedKeyComponent.getEffectiveSizeBytes();
        }
        catch (ParseException pe) {
          parseExceptionMessages.add(pe.getMessage());
        }
        if (wasNewDim) {
          // unless this is the first row we are processing, all newly discovered columns will be sparse
          if (maxIngestedEventTime != null) {
            indexer.setSparseIndexed();
          }
          if (overflow == null) {
            overflow = new ArrayList<>();
          }
          overflow.add(dimsKey);
        } else if (desc.getIndex() > dims.length || dims[desc.getIndex()] != null) {
          /*
           * index > dims.length requires that we saw this dimension and added it to the dimensionOrder map,
           * otherwise index is null. Since dims is initialized based on the size of dimensionOrder on each call to add,
           * it must have been added to dimensionOrder during this InputRow.
           *
           * if we found an index for this dimension it means we've seen it already. If !(index > dims.length) then
           * we saw it on a previous input row (this its safe to index into dims). If we found a value in
           * the dims array for this index, it means we have seen this dimension already on this input row.
           */
          throw new ISE("Dimension[%s] occurred more than once in InputRow", dimension);
        } else {
          dims[desc.getIndex()] = dimsKey;
        }
      }

      // process any dimensions with missing values in the row
      for (String missing : absentDimensions) {
        dimensionDescs.get(missing).getIndexer().setSparseIndexed();
      }
    }

    if (overflow != null) {
      // Merge overflow and non-overflow
      Object[] newDims = new Object[dims.length + overflow.size()];
      System.arraycopy(dims, 0, newDims, 0, dims.length);
      for (int i = 0; i < overflow.size(); ++i) {
        newDims[dims.length + i] = overflow.get(i);
      }
      dims = newDims;
    }

    long truncated = 0;
    if (row.getTimestamp() != null) {
      truncated = queryGranularity.bucketStart(row.getTimestampFromEpoch());
    }
    IncrementalIndexRow incrementalIndexRow = IncrementalIndexRow.createTimeAndDimswithDimsKeySize(
        Math.max(truncated, minTimestamp),
        dims,
        dimensionDescsList,
        dimsKeySize
    );
    return new IncrementalIndexRowResult(incrementalIndexRow, parseExceptionMessages);
  }

  @Nullable
  public static ParseException getCombinedParseException(
      InputRow row,
      @Nullable List<String> dimParseExceptionMessages,
      @Nullable List<String> aggParseExceptionMessages
  )
  {
    int numAdded = 0;
    StringBuilder stringBuilder = new StringBuilder();
    final List<String> details = new ArrayList<>();
    if (dimParseExceptionMessages != null) {
      details.addAll(dimParseExceptionMessages);
      for (String parseExceptionMessage : dimParseExceptionMessages) {
        stringBuilder.append(parseExceptionMessage);
        stringBuilder.append(",");
        numAdded++;
      }
    }
    if (aggParseExceptionMessages != null) {
      details.addAll(aggParseExceptionMessages);
      for (String parseExceptionMessage : aggParseExceptionMessages) {
        stringBuilder.append(parseExceptionMessage);
        stringBuilder.append(",");
        numAdded++;
      }
    }

    if (numAdded == 0) {
      return null;
    }

    // remove extra "," at the end of the message
    int messageLen = stringBuilder.length();
    if (messageLen > 0) {
      stringBuilder.delete(messageLen - 1, messageLen);
    }
    final String eventString = getSimplifiedEventStringFromRow(row);
    return new UnparseableColumnsParseException(
        eventString,
        details,
        true,
        "Found unparseable columns in row: [%s], exceptions: [%s]",
        getSimplifiedEventStringFromRow(row),
        stringBuilder.toString()
    );
  }


  private synchronized void updateMaxIngestedTime(DateTime eventTime)
  {
    if (maxIngestedEventTime == null || maxIngestedEventTime.isBefore(eventTime)) {
      maxIngestedEventTime = eventTime;
    }
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

  AtomicInteger getNumEntries()
  {
    return numEntries;
  }

  public AtomicLong getBytesInMemory()
  {
    return bytesInMemory;
  }

  private long getMinTimeMillis()
  {
    return getFacts().getMinTimeMillis();
  }

  private long getMaxTimeMillis()
  {
    return getFacts().getMaxTimeMillis();
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  /**
   * Returns dimensionsSpec from the ingestionSpec.
   */
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  /**
   * Returns names of dimension columns.
   *
   * @param includeTime whether to include {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  @Override
  public List<String> getDimensionNames(final boolean includeTime)
  {
    synchronized (dimensionDescs) {
      if (includeTime) {
        final ImmutableList.Builder<String> listBuilder =
            ImmutableList.builderWithExpectedSize(dimensionDescs.size() + 1);
        int i = 0;
        if (i == timePosition) {
          listBuilder.add(ColumnHolder.TIME_COLUMN_NAME);
        }
        for (String dimName : dimensionDescs.keySet()) {
          listBuilder.add(dimName);
          i++;
          if (i == timePosition) {
            listBuilder.add(ColumnHolder.TIME_COLUMN_NAME);
          }
        }
        return listBuilder.build();
      } else {
        return ImmutableList.copyOf(dimensionDescs.keySet());
      }
    }
  }

  /**
   * Returns a descriptor for each dimension. Does not inclue {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  @Override
  public List<DimensionDesc> getDimensions()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.values());
    }
  }

  /**
   * Returns the descriptor for a particular dimension.
   */
  @Override
  @Nullable
  public DimensionDesc getDimension(String dimension)
  {
    synchronized (dimensionDescs) {
      return dimensionDescs.get(dimension);
    }
  }

  @Override
  @Nullable
  public MetricDesc getMetric(String metric)
  {
    return metricDescs.get(metric);
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return metadata.getOrdering();
  }

  @Override
  public int getTimePosition()
  {
    return timePosition;
  }

  public static ColumnValueSelector<?> makeMetricColumnValueSelector(
      IncrementalIndexRowSelector rowSelector,
      IncrementalIndexRowHolder currEntry,
      String metric
  )
  {
    final MetricDesc metricDesc = rowSelector.getMetric(metric);
    if (metricDesc == null) {
      return NilColumnValueSelector.instance();
    }
    int metricIndex = metricDesc.getIndex();
    switch (metricDesc.getCapabilities().getType()) {
      case COMPLEX:
        return new ObjectMetricColumnSelector(rowSelector, currEntry, metricDesc);
      case LONG:
        return new LongMetricColumnSelector(rowSelector, currEntry, metricIndex);
      case FLOAT:
        return new FloatMetricColumnSelector(rowSelector, currEntry, metricIndex);
      case DOUBLE:
        return new DoubleMetricColumnSelector(rowSelector, currEntry, metricIndex);
      case STRING:
        throw new IllegalStateException("String is not a metric column type");
      default:
        throw new ISE("Unknown metric value type: %s", metricDesc.getCapabilities().getType());
    }
  }

  public Interval getInterval()
  {
    DateTime min = DateTimes.utc(minTimestamp);
    return new Interval(min, isEmpty() ? min : queryGranularity.increment(DateTimes.utc(getMaxTimeMillis())));
  }

  @Nullable
  public DateTime getMinTime()
  {
    return isEmpty() ? null : DateTimes.utc(getMinTimeMillis());
  }

  @Nullable
  public DateTime getMaxTime()
  {
    return isEmpty() ? null : DateTimes.utc(getMaxTimeMillis());
  }

  /**
   * Returns names of time and dimension columns, in persist sort order. Includes {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  public List<String> getDimensionOrder()
  {
    return getDimensionNames(true);
  }

  public static ColumnCapabilitiesImpl makeDefaultCapabilitiesFromValueType(ColumnType type)
  {
    switch (type.getType()) {
      case STRING:
        // we start out as not having multiple values, but this might change as we encounter them
        return new ColumnCapabilitiesImpl().setType(type)
                                           .setHasBitmapIndexes(true)
                                           .setDictionaryEncoded(true)
                                           .setDictionaryValuesUnique(true)
                                           .setDictionaryValuesSorted(false);
      case COMPLEX:
        return ColumnCapabilitiesImpl.createDefault().setType(type).setHasNulls(true);
      default:
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
    }
  }

  /**
   * Currently called to initialize IncrementalIndex dimension order during index creation
   * Index dimension ordering could be changed to initialize from DimensionsSpec after resolution of
   * https://github.com/apache/druid/issues/2011
   *
   * @param oldDimensionOrder dimension order to initialize
   * @param oldColumnFormats  formats for the dimensions
   */
  public void loadDimensionIterable(
      Iterable<String> oldDimensionOrder,
      Map<String, ColumnFormat> oldColumnFormats
  )
  {
    synchronized (dimensionDescs) {
      if (numRows() != 0) {
        throw new ISE("Cannot load dimension order[%s] when existing index is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        // Skip __time; its position is solely based on configuration at index creation time.
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(dim) && dimensionDescs.get(dim) == null) {
          ColumnFormat format = oldColumnFormats.get(dim);
          addNewDimension(dim, format.getColumnHandler(dim));
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(String dim, DimensionHandler handler)
  {
    DimensionDesc desc = initDimension(dimensionDescs.size(), dim, handler);
    dimensionDescs.put(dim, desc);
    dimensionDescsList.add(desc);
    return desc;
  }

  private DimensionDesc initDimension(int dimensionIndex, String dimensionName, DimensionHandler dimensionHandler)
  {
    return new DimensionDesc(dimensionIndex, dimensionName, dimensionHandler);
  }

  @Override
  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  /**
   * Returns all column names, including {@link ColumnHolder#TIME_COLUMN_NAME}.
   */
  public List<String> getColumnNames()
  {
    List<String> columnNames = new ArrayList<>(getDimensionNames(true));
    columnNames.addAll(getMetricNames());
    return columnNames;
  }

  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Iterator<Row> iterator()
  {
    return iterableWithPostAggregations(null, false).iterator();
  }

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime;
  }

  protected ColumnSelectorFactory makeColumnSelectorFactory(
      @Nullable final AggregatorFactory agg,
      final InputRowHolder in
  )
  {
    return makeColumnSelectorFactory(virtualColumns, in, agg);
  }

  protected final Comparator<IncrementalIndexRow> dimsComparator()
  {
    return new IncrementalIndexRowComparator(timePosition, dimensionDescsList);
  }

  private static String getSimplifiedEventStringFromRow(InputRow inputRow)
  {
    if (inputRow instanceof MapBasedInputRow) {
      return ((MapBasedInputRow) inputRow).getEvent().toString();
    }

    if (inputRow instanceof ListBasedInputRow) {
      return ((ListBasedInputRow) inputRow).asMap().toString();
    }

    if (inputRow instanceof TransformedInputRow) {
      InputRow innerRow = ((TransformedInputRow) inputRow).getBaseRow();
      return getSimplifiedEventStringFromRow(innerRow);
    }

    return inputRow.toString();
  }

  private static AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  private static boolean allNull(Object[] dims, int startPosition)
  {
    for (int i = startPosition; i < dims.length; i++) {
      if (dims[i] != null) {
        return false;
      }
    }
    return true;
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final DimensionHandler<?, ?, ?> handler;
    private final DimensionIndexer<?, ?, ?> indexer;

    public DimensionDesc(int index, String name, DimensionHandler<?, ?, ?> handler)
    {
      this.index = index;
      this.name = name;
      this.handler = handler;
      this.indexer = handler.makeIndexer();
    }

    public DimensionDesc(int index, String name, DimensionHandler<?, ?, ?> handler, DimensionIndexer<?, ?, ?> indexer)
    {
      this.index = index;
      this.name = name;
      this.handler = handler;
      this.indexer = indexer;
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public ColumnCapabilities getCapabilities()
    {
      return indexer.getColumnCapabilities();
    }

    public DimensionHandler<?, ?, ?> getHandler()
    {
      return handler;
    }

    public DimensionIndexer<?, ?, ?> getIndexer()
    {
      return indexer;
    }
  }

  public static final class MetricDesc
  {
    private final int index;
    private final String name;
    private final String type;
    private final ColumnCapabilities capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();

      ColumnType valueType = factory.getIntermediateType();

      if (valueType.isNumeric()) {
        capabilities = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
        this.type = valueType.toString();
      } else if (valueType.is(ValueType.COMPLEX)) {
        capabilities = ColumnCapabilitiesImpl.createDefault()
                                             .setType(valueType)
                                             .setHasNulls(ColumnCapabilities.Capable.TRUE);
        ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(valueType.getComplexTypeName());
        if (serde != null) {
          this.type = serde.getTypeName();
        } else {
          throw new ISE("Unable to handle complex type[%s]", valueType);
        }
      } else {
        // if we need to handle non-numeric and non-complex types (e.g. strings, arrays) it should be done here
        // and we should determine the appropriate ColumnCapabilities
        throw new ISE("Unable to handle type[%s] for AggregatorFactory[%s]", valueType, factory.getClass());
      }
    }

    public int getIndex()
    {
      return index;
    }

    public String getName()
    {
      return name;
    }

    public String getType()
    {
      return type;
    }

    public ColumnCapabilities getCapabilities()
    {
      return capabilities;
    }
  }

  public static class AddToFactsResult
  {
    private final int rowCount;
    private final long bytesInMemory;
    private final List<String> parseExceptionMessages;

    public AddToFactsResult(
        int rowCount,
        long bytesInMemory,
        List<String> parseExceptionMessages
    )
    {
      this.rowCount = rowCount;
      this.bytesInMemory = bytesInMemory;
      this.parseExceptionMessages = parseExceptionMessages;
    }

    int getRowCount()
    {
      return rowCount;
    }

    public long getBytesInMemory()
    {
      return bytesInMemory;
    }

    public List<String> getParseExceptionMessages()
    {
      return parseExceptionMessages;
    }
  }

  public static class InputRowHolder
  {
    @Nullable
    private InputRow row;
    private long rowId = -1;

    public void set(final InputRow row)
    {
      this.row = row;
      this.rowId++;
    }

    public void unset()
    {
      this.row = null;
    }

    public InputRow getRow()
    {
      return Preconditions.checkNotNull(row, "row");
    }

    public long getRowId()
    {
      return rowId;
    }
  }

  static class IncrementalIndexRowResult
  {
    private final IncrementalIndexRow incrementalIndexRow;
    private final List<String> parseExceptionMessages;

    IncrementalIndexRowResult(IncrementalIndexRow incrementalIndexRow, List<String> parseExceptionMessages)
    {
      this.incrementalIndexRow = incrementalIndexRow;
      this.parseExceptionMessages = parseExceptionMessages;
    }

    IncrementalIndexRow getIncrementalIndexRow()
    {
      return incrementalIndexRow;
    }

    List<String> getParseExceptionMessages()
    {
      return parseExceptionMessages;
    }
  }


  @VisibleForTesting
  static final class IncrementalIndexRowComparator implements Comparator<IncrementalIndexRow>
  {
    /**
     * Position of {@link ColumnHolder#TIME_COLUMN_NAME} in the sort order.
     */
    private final int timePosition;
    private final List<DimensionDesc> dimensionDescs;

    public IncrementalIndexRowComparator(int timePosition, List<DimensionDesc> dimDescs)
    {
      this.timePosition = timePosition;
      this.dimensionDescs = dimDescs;
    }

    @Override
    public int compare(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
    {
      int retVal = 0;

      // Number of dimension comparisons, not counting __time.
      int numDimComparisons = Math.min(lhs.dims.length, rhs.dims.length);

      int dimIndex = 0;
      while (retVal == 0 && dimIndex < numDimComparisons) {
        if (dimIndex == timePosition) {
          retVal = Longs.compare(lhs.timestamp, rhs.timestamp);

          if (retVal != 0) {
            break;
          }
        }

        final Object lhsIdxs = lhs.dims[dimIndex];
        final Object rhsIdxs = rhs.dims[dimIndex];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++dimIndex;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(dimIndex).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++dimIndex;
      }

      if (retVal == 0 && dimIndex == numDimComparisons && timePosition >= numDimComparisons) {
        retVal = Longs.compare(lhs.timestamp, rhs.timestamp);
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhs.dims.length, rhs.dims.length);
        if (lengthDiff == 0) {
          return 0;
        }
        Object[] largerDims = lengthDiff > 0 ? lhs.dims : rhs.dims;
        return allNull(largerDims, numDimComparisons) ? 0 : lengthDiff;
      }

      return retVal;
    }
  }

  private static final class LongMetricColumnSelector implements LongColumnSelector
  {
    private final IncrementalIndexRowSelector rowSelector;
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public LongMetricColumnSelector(
        IncrementalIndexRowSelector rowSelector,
        IncrementalIndexRowHolder currEntry,
        int metricIndex
    )
    {
      this.rowSelector = rowSelector;
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public long getLong()
    {
      return rowSelector.getMetricLongValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public boolean isNull()
    {
      return rowSelector.isNull(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", rowSelector);
    }
  }

  private static final class FloatMetricColumnSelector implements FloatColumnSelector
  {
    private final IncrementalIndexRowSelector rowSelector;
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public FloatMetricColumnSelector(
        IncrementalIndexRowSelector rowSelector,
        IncrementalIndexRowHolder currEntry,
        int metricIndex
    )
    {
      this.currEntry = currEntry;
      this.rowSelector = rowSelector;
      this.metricIndex = metricIndex;
    }

    @Override
    public float getFloat()
    {
      return rowSelector.getMetricFloatValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", rowSelector);
    }

    @Override
    public boolean isNull()
    {
      return rowSelector.isNull(currEntry.get().getRowIndex(), metricIndex);
    }
  }

  private static final class DoubleMetricColumnSelector implements DoubleColumnSelector
  {
    private final IncrementalIndexRowSelector rowSelector;
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public DoubleMetricColumnSelector(
        IncrementalIndexRowSelector rowSelector,
        IncrementalIndexRowHolder currEntry,
        int metricIndex
    )
    {
      this.currEntry = currEntry;
      this.rowSelector = rowSelector;
      this.metricIndex = metricIndex;
    }

    @Override
    public double getDouble()
    {
      assert !isNull();
      return rowSelector.getMetricDoubleValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public boolean isNull()
    {
      return rowSelector.isNull(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", rowSelector);
    }
  }

  private static final class ObjectMetricColumnSelector extends ObjectColumnSelector
  {
    private final IncrementalIndexRowSelector rowSelector;
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;
    private final Class<?> classOfObject;

    public ObjectMetricColumnSelector(
        IncrementalIndexRowSelector rowSelector,
        IncrementalIndexRowHolder currEntry,
        MetricDesc metricDesc
    )
    {
      this.currEntry = currEntry;
      this.rowSelector = rowSelector;
      this.metricIndex = metricDesc.getIndex();
      this.classOfObject = ComplexMetrics.getSerdeForType(metricDesc.getType()).getObjectStrategy().getClazz();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return rowSelector.getMetricObjectValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public Class<?> classOfObject()
    {
      return classOfObject;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", rowSelector);
    }
  }
}
