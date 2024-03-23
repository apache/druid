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
import org.apache.druid.common.config.NullHandling;
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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
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
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.transform.TransformedInputRow;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory, row-based data structure used to hold data during ingestion. Realtime tasks query this index using
 * {@link IncrementalIndexStorageAdapter}.
 *
 * Concurrency model: {@link #add(InputRow)} and {@link #add(InputRow, boolean)} are not thread-safe, and must be
 * called from a single thread or externally synchronized. However, the methods that support
 * {@link IncrementalIndexStorageAdapter} are thread-safe, and may be called concurrently with each other, and with
 * the "add" methods. This concurrency model supports real-time queries of the data in the index.
 */
public abstract class IncrementalIndex implements Iterable<Row>, Closeable, ColumnInspector
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
  private final Granularity gran;
  private final boolean rollup;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final VirtualColumns virtualColumns;
  private final AggregatorFactory[] metrics;
  private final Metadata metadata;
  protected final boolean preserveExistingMetrics;

  private final Map<String, MetricDesc> metricDescs;

  private final DimensionsSpec dimensionsSpec;
  private final Map<String, DimensionDesc> dimensionDescs;
  private final List<DimensionDesc> dimensionDescsList;
  // dimension capabilities are provided by the indexers
  private final Map<String, ColumnCapabilities> timeAndMetricsColumnCapabilities;
  private final Map<String, ColumnFormat> timeAndMetricsColumnFormats;
  private final AtomicInteger numEntries = new AtomicInteger();
  private final AtomicLong bytesInMemory = new AtomicLong();
  private final boolean useMaxMemoryEstimates;

  private final boolean useSchemaDiscovery;

  private final InputRowHolder inputRowHolder = new InputRowHolder();

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
   * @param useMaxMemoryEstimates     true if max values should be used to estimate memory
   */
  protected IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean preserveExistingMetrics,
      final boolean useMaxMemoryEstimates
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.rollup = incrementalIndexSchema.isRollup();
    this.virtualColumns = incrementalIndexSchema.getVirtualColumns();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.preserveExistingMetrics = preserveExistingMetrics;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    this.useSchemaDiscovery = incrementalIndexSchema.getDimensionsSpec()
                                                    .useSchemaDiscovery();

    this.timeAndMetricsColumnCapabilities = new HashMap<>();
    this.timeAndMetricsColumnFormats = new HashMap<>();
    this.metricDescs = Maps.newLinkedHashMap();
    this.dimensionDescs = Maps.newLinkedHashMap();
    this.metadata = new Metadata(
        null,
        getCombiningAggregators(metrics),
        incrementalIndexSchema.getTimestampSpec(),
        this.gran,
        this.rollup
    );

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
    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      addNewDimension(dimSchema.getName(), dimSchema.getDimensionHandler());
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
  }

  public abstract FactsHolder getFacts();

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract void initAggs(
      AggregatorFactory[] metrics,
      InputRowHolder rowSupplier
  );

  // Note: This method does not need to be thread safe.
  protected abstract AddToFactsResult addToFacts(
      IncrementalIndexRow key,
      InputRowHolder inputRowHolder,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException;

  public abstract int getLastRowIndex();

  protected abstract float getMetricFloatValue(int rowOffset, int aggOffset);

  protected abstract long getMetricLongValue(int rowOffset, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowOffset, int aggOffset);

  protected abstract double getMetricDoubleValue(int rowOffset, int aggOffset);

  protected abstract boolean isNull(int rowOffset, int aggOffset);

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

  static class AddToFactsResult
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
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow. If any parse failure occurs, a {@link ParseException} is returned in {@link IncrementalIndexAddResult}.
   *
   * @throws IndexSizeExceededException this exception is thrown once it reaches max rows limit and skipMaxRowsInMemoryCheck is set to false.
   */
  public IncrementalIndexAddResult add(InputRow row) throws IndexSizeExceededException
  {
    return add(row, false);
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   *
   * Not thread-safe.
   *
   * @param row                      the row of data to add
   * @param skipMaxRowsInMemoryCheck whether or not to skip the check of rows exceeding the max rows or bytes limit
   *
   * @return the number of rows in the data set after adding the InputRow. If any parse failure occurs, a {@link ParseException} is returned in {@link IncrementalIndexAddResult}.
   *
   * @throws IndexSizeExceededException this exception is thrown once it reaches max rows limit and skipMaxRowsInMemoryCheck is set to false.
   */
  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck)
      throws IndexSizeExceededException
  {
    IncrementalIndexRowResult incrementalIndexRowResult = toIncrementalIndexRow(row);
    inputRowHolder.set(row);
    final AddToFactsResult addToFactsResult = addToFacts(
        incrementalIndexRowResult.getIncrementalIndexRow(),
        inputRowHolder,
        skipMaxRowsInMemoryCheck
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
        if (Strings.isNullOrEmpty(dimension)) {
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
      truncated = gran.bucketStart(row.getTimestampFromEpoch());
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

  private synchronized void updateMaxIngestedTime(DateTime eventTime)
  {
    if (maxIngestedEventTime == null || maxIngestedEventTime.isBefore(eventTime)) {
      maxIngestedEventTime = eventTime;
    }
  }

  public boolean isEmpty()
  {
    return numEntries.get() == 0;
  }

  public int size()
  {
    return numEntries.get();
  }

  AtomicInteger getNumEntries()
  {
    return numEntries;
  }

  AggregatorFactory[] getMetrics()
  {
    return metrics;
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

  public List<String> getDimensionNames()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
  }

  public List<DimensionDesc> getDimensions()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.values());
    }
  }

  @Nullable
  public DimensionDesc getDimension(String dimension)
  {
    synchronized (dimensionDescs) {
      return dimensionDescs.get(dimension);
    }
  }

  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    MetricDesc metricDesc = metricDescs.get(metric);
    if (metricDesc == null) {
      return NilColumnValueSelector.instance();
    }
    int metricIndex = metricDesc.getIndex();
    switch (metricDesc.getCapabilities().getType()) {
      case COMPLEX:
        return new ObjectMetricColumnSelector(metricDesc, currEntry, metricIndex);
      case LONG:
        return new LongMetricColumnSelector(currEntry, metricIndex);
      case FLOAT:
        return new FloatMetricColumnSelector(currEntry, metricIndex);
      case DOUBLE:
        return new DoubleMetricColumnSelector(currEntry, metricIndex);
      case STRING:
        throw new IllegalStateException("String is not a metric column type");
      default:
        throw new ISE("Unknown metric value type: %s", metricDesc.getCapabilities().getType());
    }
  }

  public Interval getInterval()
  {
    DateTime min = DateTimes.utc(minTimestamp);
    return new Interval(min, isEmpty() ? min : gran.increment(DateTimes.utc(getMaxTimeMillis())));
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

  @Nullable
  public Integer getDimensionIndex(String dimension)
  {
    DimensionDesc dimSpec = getDimension(dimension);
    return dimSpec == null ? null : dimSpec.getIndex();
  }

  public List<String> getDimensionOrder()
  {
    synchronized (dimensionDescs) {
      return ImmutableList.copyOf(dimensionDescs.keySet());
    }
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
   */
  public void loadDimensionIterable(
      Iterable<String> oldDimensionOrder,
      Map<String, ColumnFormat> oldColumnCapabilities
  )
  {
    synchronized (dimensionDescs) {
      if (!dimensionDescs.isEmpty()) {
        throw new ISE("Cannot load dimension order when existing order[%s] is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        if (dimensionDescs.get(dim) == null) {
          ColumnFormat format = oldColumnCapabilities.get(dim);
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
    return new DimensionDesc(dimensionIndex, dimensionName, dimensionHandler, useMaxMemoryEstimates);
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  public List<String> getColumnNames()
  {
    List<String> columnNames = new ArrayList<>(getDimensionNames());
    columnNames.addAll(getMetricNames());
    return columnNames;
  }

  public Metadata getMetadata()
  {
    return metadata;
  }

  private static AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  @Override
  public Iterator<Row> iterator()
  {
    return iterableWithPostAggregations(null, false).iterator();
  }

  public abstract Iterable<Row> iterableWithPostAggregations(
      @Nullable List<PostAggregator> postAggs,
      boolean descending
  );

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime;
  }

  public static final class DimensionDesc
  {
    private final int index;
    private final String name;
    private final DimensionHandler handler;
    private final DimensionIndexer indexer;

    public DimensionDesc(int index, String name, DimensionHandler handler, boolean useMaxMemoryEstimates)
    {
      this.index = index;
      this.name = name;
      this.handler = handler;
      this.indexer = handler.makeIndexer(useMaxMemoryEstimates);
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

    public DimensionHandler getHandler()
    {
      return handler;
    }

    public DimensionIndexer getIndexer()
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

  protected ColumnSelectorFactory makeColumnSelectorFactory(
      @Nullable final AggregatorFactory agg,
      final InputRowHolder in
  )
  {
    return makeColumnSelectorFactory(virtualColumns, in, agg);
  }

  protected final Comparator<IncrementalIndexRow> dimsComparator()
  {
    return new IncrementalIndexRowComparator(dimensionDescsList);
  }

  @VisibleForTesting
  static final class IncrementalIndexRowComparator implements Comparator<IncrementalIndexRow>
  {
    private List<DimensionDesc> dimensionDescs;

    public IncrementalIndexRowComparator(List<DimensionDesc> dimDescs)
    {
      this.dimensionDescs = dimDescs;
    }

    @Override
    public int compare(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
    {
      int retVal = Longs.compare(lhs.timestamp, rhs.timestamp);
      int numComparisons = Math.min(lhs.dims.length, rhs.dims.length);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.dims[index];
        final Object rhsIdxs = rhs.dims[index];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescs.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhs.dims.length, rhs.dims.length);
        if (lengthDiff == 0) {
          return 0;
        }
        Object[] largerDims = lengthDiff > 0 ? lhs.dims : rhs.dims;
        return allNull(largerDims, numComparisons) ? 0 : lengthDiff;
      }

      return retVal;
    }
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

  public interface FactsHolder
  {
    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int getPriorIndex(IncrementalIndexRow key);

    long getMinTimeMillis();

    long getMaxTimeMillis();

    Iterator<IncrementalIndexRow> iterator(boolean descending);

    Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

    Iterable<IncrementalIndexRow> keySet();

    /**
     * Get all {@link IncrementalIndexRow} to persist, ordered with {@link Comparator<IncrementalIndexRow>}
     *
     * @return
     */
    Iterable<IncrementalIndexRow> persistIterable();

    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int putIfAbsent(IncrementalIndexRow key, int rowIndex);

    void clear();
  }

  private final class LongMetricColumnSelector implements LongColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public LongMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public long getLong()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricLongValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get().getRowIndex(), metricIndex);
    }
  }

  private final class ObjectMetricColumnSelector extends ObjectColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;
    private Class classOfObject;

    public ObjectMetricColumnSelector(
        MetricDesc metricDesc,
        IncrementalIndexRowHolder currEntry,
        int metricIndex
    )
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
      classOfObject = ComplexMetrics.getSerdeForType(metricDesc.getType()).getObjectStrategy().getClazz();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return getMetricObjectValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public Class classOfObject()
    {
      return classOfObject;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }
  }

  private final class FloatMetricColumnSelector implements FloatColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public FloatMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public float getFloat()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricFloatValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get().getRowIndex(), metricIndex);
    }
  }

  private final class DoubleMetricColumnSelector implements DoubleColumnSelector
  {
    private final IncrementalIndexRowHolder currEntry;
    private final int metricIndex;

    public DoubleMetricColumnSelector(IncrementalIndexRowHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public double getDouble()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricDoubleValue(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get().getRowIndex(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }
  }
}
