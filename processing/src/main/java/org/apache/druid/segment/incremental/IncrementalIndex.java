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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.SpatialDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractIndex;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public abstract class IncrementalIndex<AggregatorType> extends AbstractIndex implements Iterable<Row>, Closeable
{
  /**
   * Column selector used at ingestion time for inputs to aggregators.
   *
   * @param agg                       the aggregator
   * @param in                        ingestion-time input row supplier
   * @param deserializeComplexMetrics whether complex objects should be deserialized by a {@link ComplexMetricExtractor}
   *
   * @return column selector factory
   */
  public static ColumnSelectorFactory makeColumnSelectorFactory(
      final VirtualColumns virtualColumns,
      final AggregatorFactory agg,
      final Supplier<InputRow> in,
      final boolean deserializeComplexMetrics
  )
  {
    final RowBasedColumnSelectorFactory<InputRow> baseSelectorFactory = RowBasedColumnSelectorFactory.create(
        RowAdapters.standardRow(),
        in::get,
        RowSignature.empty(),
        true
    );

    class IncrementalIndexInputRowColumnSelectorFactory implements ColumnSelectorFactory
    {
      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(final String column)
      {
        final boolean isComplexMetric = ValueType.COMPLEX.equals(agg.getType());

        final ColumnValueSelector selector = baseSelectorFactory.makeColumnValueSelector(column);

        if (!isComplexMetric || !deserializeComplexMetrics) {
          return selector;
        } else {
          // Wrap selector in a special one that uses ComplexMetricSerde to modify incoming objects.
          // For complex aggregators that read from multiple columns, we wrap all of them. This is not ideal but it
          // has worked so far.
          final String complexTypeName = agg.getComplexTypeName();
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
              return extractor.extractValue(in.get(), column, agg);
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("in", in);
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
  private final AggregatorType[] aggs;
  private final boolean deserializeComplexMetrics;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;

  private final Map<String, DimensionDesc> dimensionDescs;
  private final List<DimensionDesc> dimensionDescsList;
  // dimension capabilities are provided by the indexers
  private final Map<String, ColumnCapabilities> timeAndMetricsColumnCapabilities;
  private final AtomicInteger numEntries = new AtomicInteger();
  private final AtomicLong bytesInMemory = new AtomicLong();

  // This is modified on add() in a critical section.
  private final ThreadLocal<InputRow> in = new ThreadLocal<>();
  private final Supplier<InputRow> rowSupplier = in::get;

  private volatile DateTime maxIngestedEventTime;


  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   * <p>
   * Set concurrentEventAdd to true to indicate that adding of input row should be thread-safe (for example, groupBy
   * where the multiple threads can add concurrently to the IncrementalIndex).
   *
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   * @param concurrentEventAdd        flag whether ot not adding of input rows should be thread-safe
   */
  protected IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.rollup = incrementalIndexSchema.isRollup();
    this.virtualColumns = incrementalIndexSchema.getVirtualColumns();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.deserializeComplexMetrics = deserializeComplexMetrics;

    this.timeAndMetricsColumnCapabilities = new HashMap<>();
    this.metadata = new Metadata(
        null,
        getCombiningAggregators(metrics),
        incrementalIndexSchema.getTimestampSpec(),
        this.gran,
        this.rollup
    );

    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics, concurrentEventAdd);

    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      timeAndMetricsColumnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }

    DimensionsSpec dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();
    this.dimensionDescs = Maps.newLinkedHashMap();

    this.dimensionDescsList = new ArrayList<>();
    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ValueType type = dimSchema.getValueType();
      String dimName = dimSchema.getName();

      // Note: Things might be simpler if DimensionSchema had a method "getColumnCapabilities()" which could return
      // type specific capabilities by itself. However, for various reasons, DimensionSchema currently lives in druid-core
      // while ColumnCapabilities lives in druid-processing which makes that approach difficult.
      ColumnCapabilitiesImpl capabilities = makeDefaultCapabilitiesFromValueType(type, dimSchema.getTypeName());

      capabilities.setHasBitmapIndexes(dimSchema.hasBitmapIndex());

      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      }
      DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(
          dimName,
          capabilities,
          dimSchema.getMultiValueHandling()
      );
      addNewDimension(dimName, handler);
    }

    //__time capabilities
    timeAndMetricsColumnCapabilities.put(
        ColumnHolder.TIME_COLUMN_NAME,
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG)
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

  protected abstract AggregatorType[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  );

  // Note: This method needs to be thread safe.
  protected abstract AddToFactsResult addToFacts(
      InputRow row,
      IncrementalIndexRow key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException;

  public abstract int getLastRowIndex();

  protected abstract AggregatorType[] getAggsForRow(int rowOffset);

  protected abstract Object getAggVal(AggregatorType agg, int rowOffset, int aggPosition);

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

  public Map<String, ColumnCapabilities> getColumnCapabilities()
  {
    ImmutableMap.Builder<String, ColumnCapabilities> builder =
        ImmutableMap.<String, ColumnCapabilities>builder().putAll(timeAndMetricsColumnCapabilities);

    dimensionDescs.forEach((dimension, desc) -> builder.put(dimension, desc.getCapabilities()));
    return builder.build();
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p>
   * <p>
   * Calls to add() are thread safe.
   * <p>
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow. If any parse failure occurs, a {@link ParseException} is returned in {@link IncrementalIndexAddResult}.
   */
  public IncrementalIndexAddResult add(InputRow row) throws IndexSizeExceededException
  {
    return add(row, false);
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p>
   * <p>
   * Calls to add() are thread safe.
   * <p>
   *
   * @param row the row of data to add
   * @param skipMaxRowsInMemoryCheck whether or not skip the check of rows exceeding the max rows limit
   * @return the number of rows in the data set after adding the InputRow. If any parse failure occurs, a {@link ParseException} is returned in {@link IncrementalIndexAddResult}.
   * @throws IndexSizeExceededException this exception is thrown once it reaches max rows limit and skipMaxRowsInMemoryCheck is set to false.
   */
  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    IncrementalIndexRowResult incrementalIndexRowResult = toIncrementalIndexRow(row);
    final AddToFactsResult addToFactsResult = addToFacts(
        row,
        incrementalIndexRowResult.getIncrementalIndexRow(),
        in,
        rowSupplier,
        skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    @Nullable ParseException parseException = getCombinedParseException(
        row,
        incrementalIndexRowResult.getParseExceptionMessages(),
        addToFactsResult.getParseExceptionMessages()
    );
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
          desc = addNewDimension(
              dimension,
              DimensionHandlerUtils.getHandlerFromCapabilities(
                  dimension,
                  // for schemaless type discovery, everything is a String. this should probably try to autodetect
                  // based on the value to use a better handler
                  makeDefaultCapabilitiesFromValueType(ValueType.STRING, null),
                  null
              )
          );
        }
        DimensionIndexer indexer = desc.getIndexer();
        Object dimsKey = null;
        try {
          dimsKey = indexer.processRowValsToUnsortedEncodedKeyComponent(row.getRaw(dimension), true);
        }
        catch (ParseException pe) {
          parseExceptionMessages.add(pe.getMessage());
        }
        dimsKeySize += indexer.estimateEncodedKeyComponentSize(dimsKey);
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
      truncated = gran.bucketStart(row.getTimestamp()).getMillis();
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

    if (dimParseExceptionMessages != null) {
      for (String parseExceptionMessage : dimParseExceptionMessages) {
        stringBuilder.append(parseExceptionMessage);
        stringBuilder.append(",");
        numAdded++;
      }
    }
    if (aggParseExceptionMessages != null) {
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
    return new ParseException(
        true,
        "Found unparseable columns in row: [%s], exceptions: [%s]",
        row,
        stringBuilder.toString()
    );
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

  boolean getDeserializeComplexMetrics()
  {
    return deserializeComplexMetrics;
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

  public AggregatorType[] getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
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

  @Nullable
  public String getMetricType(String metric)
  {
    final MetricDesc metricDesc = metricDescs.get(metric);
    return metricDesc != null ? metricDesc.getType() : null;
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

  private ColumnCapabilitiesImpl makeDefaultCapabilitiesFromValueType(ValueType type, String typeName)
  {
    switch (type) {
      case STRING:
        // we start out as not having multiple values, but this might change as we encounter them
        return new ColumnCapabilitiesImpl().setType(type)
                                           .setHasBitmapIndexes(true)
                                           .setDictionaryEncoded(true)
                                           .setDictionaryValuesUnique(true)
                                           .setDictionaryValuesSorted(false);
      case COMPLEX:
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type).setHasNulls(true).setComplexTypeName(typeName);
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
      Map<String, ColumnCapabilities> oldColumnCapabilities
  )
  {
    synchronized (dimensionDescs) {
      if (!dimensionDescs.isEmpty()) {
        throw new ISE("Cannot load dimension order when existing order[%s] is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        if (dimensionDescs.get(dim) == null) {
          ColumnCapabilitiesImpl capabilities = ColumnCapabilitiesImpl.snapshot(
              oldColumnCapabilities.get(dim),
              IndexMergerV9.DIMENSION_CAPABILITY_MERGE_LOGIC
          );
          DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
          addNewDimension(dim, handler);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(String dim, DimensionHandler handler)
  {
    DimensionDesc desc = new DimensionDesc(dimensionDescs.size(), dim, handler);
    dimensionDescs.put(dim, desc);
    dimensionDescsList.add(desc);
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  @Override
  public List<String> getColumnNames()
  {
    List<String> columnNames = new ArrayList<>(getDimensionNames());
    columnNames.addAll(getMetricNames());
    return columnNames;
  }

  @Override
  public StorageAdapter toStorageAdapter()
  {
    return new IncrementalIndexStorageAdapter(this);
  }

  @Nullable
  public ColumnCapabilities getCapabilities(String column)
  {
    if (dimensionDescs.containsKey(column)) {
      return dimensionDescs.get(column).getCapabilities();
    }
    return timeAndMetricsColumnCapabilities.get(column);
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

  public Iterable<Row> iterableWithPostAggregations(
      @Nullable final List<PostAggregator> postAggs,
      final boolean descending
  )
  {
    return () -> {
      final List<DimensionDesc> dimensions = getDimensions();

      return Iterators.transform(
          getFacts().iterator(descending),
          incrementalIndexRow -> {
            final int rowOffset = incrementalIndexRow.getRowIndex();

            Object[] theDims = incrementalIndexRow.getDims();

            Map<String, Object> theVals = Maps.newLinkedHashMap();
            for (int i = 0; i < theDims.length; ++i) {
              Object dim = theDims[i];
              DimensionDesc dimensionDesc = dimensions.get(i);
              if (dimensionDesc == null) {
                continue;
              }
              String dimensionName = dimensionDesc.getName();
              DimensionHandler handler = dimensionDesc.getHandler();
              if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
                theVals.put(dimensionName, null);
                continue;
              }
              final DimensionIndexer indexer = dimensionDesc.getIndexer();
              Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualList(dim);
              theVals.put(dimensionName, rowVals);
            }

            AggregatorType[] aggs = getAggsForRow(rowOffset);
            for (int i = 0; i < aggs.length; ++i) {
              theVals.put(metrics[i].getName(), getAggVal(aggs[i], rowOffset, i));
            }

            if (postAggs != null) {
              for (PostAggregator postAgg : postAggs) {
                theVals.put(postAgg.getName(), postAgg.compute(theVals));
              }
            }

            return new MapBasedRow(incrementalIndexRow.getTimestamp(), theVals);
          }
      );
    };
  }

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

    public DimensionDesc(int index, String name, DimensionHandler handler)
    {
      this.index = index;
      this.name = name;
      this.handler = handler;
      this.indexer = handler.makeIndexer();
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

      ValueType valueType = factory.getType();

      if (valueType.isNumeric()) {
        capabilities = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
        this.type = valueType.toString();
      } else if (ValueType.COMPLEX.equals(valueType)) {
        capabilities = ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.COMPLEX)
                                             .setHasNulls(ColumnCapabilities.Capable.TRUE);
        String complexTypeName = factory.getComplexTypeName();
        ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(complexTypeName);
        if (serde != null) {
          this.type = serde.getTypeName();
        } else {
          throw new ISE("Unable to handle complex type[%s] of type[%s]", complexTypeName, valueType);
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
      final AggregatorFactory agg,
      final Supplier<InputRow> in,
      final boolean deserializeComplexMetrics
  )
  {
    return makeColumnSelectorFactory(virtualColumns, agg, in, deserializeComplexMetrics);
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

  interface FactsHolder
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

  static class RollupFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    // Can't use Set because we need to be able to get from collection
    private final ConcurrentMap<IncrementalIndexRow, IncrementalIndexRow> facts;
    private final List<DimensionDesc> dimensionDescsList;

    RollupFactsHolder(
        boolean sortFacts,
        Comparator<IncrementalIndexRow> incrementalIndexRowComparator,
        List<DimensionDesc> dimensionDescsList
    )
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>(incrementalIndexRowComparator);
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      IncrementalIndexRow row = facts.get(key);
      return row == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : row.getRowIndex();
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).firstKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).lastKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).descendingMap()
                                                                                         .keySet()
                                                                                         .iterator();
      }
      return keySet().iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (!sortFacts) {
        throw new UnsupportedOperationException("can't get timeRange from unsorted facts data.");
      }
      IncrementalIndexRow start = new IncrementalIndexRow(timeStart, new Object[]{}, dimensionDescsList);
      IncrementalIndexRow end = new IncrementalIndexRow(timeEnd, new Object[]{}, dimensionDescsList);
      ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow> subMap =
          ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).subMap(start, end);
      ConcurrentMap<IncrementalIndexRow, IncrementalIndexRow> rangeMap = descending ? subMap.descendingMap() : subMap;
      return rangeMap.keySet();
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return facts.keySet();
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      // with rollup, facts are already pre-sorted so just return keyset
      return keySet();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      // setRowIndex() must be called before facts.putIfAbsent() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      IncrementalIndexRow prev = facts.putIfAbsent(key, key);
      return prev == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : prev.getRowIndex();
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  static class PlainFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    private final ConcurrentMap<Long, Deque<IncrementalIndexRow>> facts;

    private final Comparator<IncrementalIndexRow> incrementalIndexRowComparator;

    public PlainFactsHolder(boolean sortFacts, Comparator<IncrementalIndexRow> incrementalIndexRowComparator)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>();
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.incrementalIndexRowComparator = incrementalIndexRowComparator;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).firstKey();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).lastKey();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return timeOrderedConcat(((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts)
                                     .descendingMap().values(), true).iterator();
      }
      return timeOrderedConcat(facts.values(), false).iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>> subMap =
          ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).subMap(timeStart, timeEnd);
      final ConcurrentMap<Long, Deque<IncrementalIndexRow>> rangeMap = descending ? subMap.descendingMap() : subMap;
      return timeOrderedConcat(rangeMap.values(), descending);
    }

    private Iterable<IncrementalIndexRow> timeOrderedConcat(
        final Iterable<Deque<IncrementalIndexRow>> iterable,
        final boolean descending
    )
    {
      return () -> Iterators.concat(
          Iterators.transform(
              iterable.iterator(),
              input -> descending ? input.descendingIterator() : input.iterator()
          )
      );
    }

    private Stream<IncrementalIndexRow> timeAndDimsOrderedConcat(
        final Collection<Deque<IncrementalIndexRow>> rowGroups
    )
    {
      return rowGroups.stream()
                      .flatMap(Collection::stream)
                      .sorted(incrementalIndexRowComparator);
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return timeOrderedConcat(facts.values(), false);
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      return () -> timeAndDimsOrderedConcat(facts.values()).iterator();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      Long time = key.getTimestamp();
      Deque<IncrementalIndexRow> rows = facts.get(time);
      if (rows == null) {
        facts.putIfAbsent(time, new ConcurrentLinkedDeque<>());
        // in race condition, rows may be put by other thread, so always get latest status from facts
        rows = facts.get(time);
      }
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      rows.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  private class LongMetricColumnSelector implements LongColumnSelector
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

  private class ObjectMetricColumnSelector extends ObjectColumnSelector
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

  private class FloatMetricColumnSelector implements FloatColumnSelector
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

  private class DoubleMetricColumnSelector implements DoubleColumnSelector
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
