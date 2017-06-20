/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.collections.NonBlockingPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class IncrementalIndex<AggregatorType> implements Iterable<Row>, Closeable
{
  private volatile DateTime maxIngestedEventTime;

  // Used to discover ValueType based on the class of values in a row
  // Also used to convert between the duplicate ValueType enums in DimensionSchema (druid-api) and main druid.
  private static final Map<Object, ValueType> TYPE_MAP = ImmutableMap.<Object, ValueType>builder()
      .put(Long.class, ValueType.LONG)
      .put(Double.class, ValueType.FLOAT)
      .put(Float.class, ValueType.FLOAT)
      .put(String.class, ValueType.STRING)
      .put(DimensionSchema.ValueType.LONG, ValueType.LONG)
      .put(DimensionSchema.ValueType.FLOAT, ValueType.FLOAT)
      .put(DimensionSchema.ValueType.STRING, ValueType.STRING)
      .build();

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
    final RowBasedColumnSelectorFactory baseSelectorFactory = RowBasedColumnSelectorFactory.create(in, null);

    class IncrementalIndexInputRowColumnSelectorFactory implements ColumnSelectorFactory
    {
      @Override
      public LongColumnSelector makeLongColumnSelector(final String columnName)
      {
        return baseSelectorFactory.makeLongColumnSelector(columnName);
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(final String columnName)
      {
        return baseSelectorFactory.makeFloatColumnSelector(columnName);
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(final String column)
      {
        final String typeName = agg.getTypeName();

        final ObjectColumnSelector rawColumnSelector = baseSelectorFactory.makeObjectColumnSelector(column);

        if ((Enums.getIfPresent(ValueType.class, typeName.toUpperCase()).isPresent() && !typeName.equalsIgnoreCase(ValueType.COMPLEX.name()))
            || !deserializeComplexMetrics) {
          return rawColumnSelector;
        } else {
          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Don't know how to handle type[%s]", typeName);
          }

          final ComplexMetricExtractor extractor = serde.getExtractor();
          return new ObjectColumnSelector()
          {
            @Override
            public Class classOfObject()
            {
              return extractor.extractedClass();
            }

            @Override
            public Object get()
            {
              return extractor.extractValue(in.get(), column);
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
  private final boolean reportParseExceptions;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;

  private final Map<String, DimensionDesc> dimensionDescs;
  private final List<DimensionDesc> dimensionDescsList;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  private final ThreadLocal<InputRow> in = new ThreadLocal<>();
  private final Supplier<InputRow> rowSupplier = in::get;

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * Set concurrentEventAdd to true to indicate that adding of input row should be thread-safe (for example, groupBy
   * where the multiple threads can add concurrently to the IncrementalIndex).
   *
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   * @param reportParseExceptions     flag whether or not to report ParseExceptions that occur while extracting values
   *                                  from input rows
   * @param concurrentEventAdd        flag whether ot not adding of input rows should be thread-safe
   */
  protected IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
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
    this.reportParseExceptions = reportParseExceptions;

    this.columnCapabilities = Maps.newHashMap();
    this.metadata = new Metadata()
        .setAggregators(getCombiningAggregators(metrics))
        .setTimestampSpec(incrementalIndexSchema.getTimestampSpec())
        .setQueryGranularity(this.gran)
        .setRollup(this.rollup);

    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics, concurrentEventAdd);

    this.metricDescs = Maps.newLinkedHashMap();
    for (AggregatorFactory metric : metrics) {
      MetricDesc metricDesc = new MetricDesc(metricDescs.size(), metric);
      metricDescs.put(metricDesc.getName(), metricDesc);
      columnCapabilities.put(metricDesc.getName(), metricDesc.getCapabilities());
    }

    DimensionsSpec dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();
    this.dimensionDescs = Maps.newLinkedHashMap();

    this.dimensionDescsList = new ArrayList<>();
    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ValueType type = TYPE_MAP.get(dimSchema.getValueType());
      String dimName = dimSchema.getName();
      ColumnCapabilitiesImpl capabilities = makeCapabilitesFromValueType(type);
      if (dimSchema.getTypeName().equals(DimensionSchema.SPATIAL_TYPE_NAME)) {
        capabilities.setHasSpatialIndexes(true);
      } else {
        DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(
            dimName,
            capabilities,
            dimSchema.getMultiValueHandling()
        );
        addNewDimension(dimName, capabilities, handler);
      }
      columnCapabilities.put(dimName, capabilities);
    }

    //__time capabilities
    ColumnCapabilitiesImpl timeCapabilities = new ColumnCapabilitiesImpl();
    timeCapabilities.setType(ValueType.LONG);
    columnCapabilities.put(Column.TIME_COLUMN_NAME, timeCapabilities);

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = dimensionsSpec.getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }
  }

  public static class Builder
  {
    private IncrementalIndexSchema incrementalIndexSchema;
    private boolean deserializeComplexMetrics;
    private boolean reportParseExceptions;
    private boolean concurrentEventAdd;
    private boolean sortFacts;
    private int maxRowCount;

    public Builder()
    {
      incrementalIndexSchema = null;
      deserializeComplexMetrics = true;
      reportParseExceptions = true;
      concurrentEventAdd = false;
      sortFacts = true;
      maxRowCount = 0;
    }

    public Builder setIndexSchema(final IncrementalIndexSchema incrementalIndexSchema)
    {
      this.incrementalIndexSchema = incrementalIndexSchema;
      return this;
    }

    /**
     * A helper method to set a simple index schema with only metrics and default values for the other parameters. Note
     * that this method is normally used for testing and benchmarking; it is unlikely that you would use it in
     * production settings.
     *
     * @param metrics variable array of {@link AggregatorFactory} metrics
     *
     * @return this
     */
    @VisibleForTesting
    public Builder setSimpleTestingIndexSchema(final AggregatorFactory... metrics)
    {
      this.incrementalIndexSchema = new IncrementalIndexSchema.Builder()
          .withMetrics(metrics)
          .build();
      return this;
    }

    public Builder setDeserializeComplexMetrics(final boolean deserializeComplexMetrics)
    {
      this.deserializeComplexMetrics = deserializeComplexMetrics;
      return this;
    }

    public Builder setReportParseExceptions(final boolean reportParseExceptions)
    {
      this.reportParseExceptions = reportParseExceptions;
      return this;
    }

    public Builder setConcurrentEventAdd(final boolean concurrentEventAdd)
    {
      this.concurrentEventAdd = concurrentEventAdd;
      return this;
    }

    public Builder setSortFacts(final boolean sortFacts)
    {
      this.sortFacts = sortFacts;
      return this;
    }

    public Builder setMaxRowCount(final int maxRowCount)
    {
      this.maxRowCount = maxRowCount;
      return this;
    }

    public IncrementalIndex buildOnheap()
    {
      if (maxRowCount <= 0) {
        throw new IllegalArgumentException("Invalid max row count: " + maxRowCount);
      }

      return new OnheapIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementIndexSchema is null"),
          deserializeComplexMetrics,
          reportParseExceptions,
          concurrentEventAdd,
          sortFacts,
          maxRowCount
      );
    }

    public IncrementalIndex buildOffheap(final NonBlockingPool<ByteBuffer> bufferPool)
    {
      if (maxRowCount <= 0) {
        throw new IllegalArgumentException("Invalid max row count: " + maxRowCount);
      }

      return new OffheapIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementalIndexSchema is null"),
          deserializeComplexMetrics,
          reportParseExceptions,
          concurrentEventAdd,
          sortFacts,
          maxRowCount,
          Objects.requireNonNull(bufferPool, "bufferPool is null")
      );
    }
  }

  public boolean isRollup()
  {
    return rollup;
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
  protected abstract Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException;

  public abstract int getLastRowIndex();

  protected abstract AggregatorType[] getAggsForRow(int rowOffset);

  protected abstract Object getAggVal(AggregatorType agg, int rowOffset, int aggPosition);

  protected abstract float getMetricFloatValue(int rowOffset, int aggOffset);

  protected abstract long getMetricLongValue(int rowOffset, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowOffset, int aggOffset);

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

  private ValueType getTypeFromDimVal(Object dimVal)
  {
    Object singleVal;
    if (dimVal instanceof List) {
      List dimValList = (List) dimVal;
      singleVal = dimValList.size() == 0 ? null : dimValList.get(0);
    } else {
      singleVal = dimVal;
    }

    if (singleVal == null) {
      return null;
    }

    return TYPE_MAP.get(singleVal.getClass());
  }

  public Map<String, ColumnCapabilitiesImpl> getColumnCapabilities()
  {
    return columnCapabilities;
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
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(InputRow row) throws IndexSizeExceededException
  {
    TimeAndDims key = toTimeAndDims(row);
    final int rv = addToFacts(
        metrics,
        deserializeComplexMetrics,
        reportParseExceptions,
        row,
        numEntries,
        key,
        in,
        rowSupplier
    );
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  @VisibleForTesting
  TimeAndDims toTimeAndDims(InputRow row) throws IndexSizeExceededException
  {
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();

    Object[] dims;
    List<Object> overflow = null;
    synchronized (dimensionDescs) {
      dims = new Object[dimensionDescs.size()];
      for (String dimension : rowDimensions) {
        boolean wasNewDim = false;
        ColumnCapabilitiesImpl capabilities;
        DimensionDesc desc = dimensionDescs.get(dimension);
        if (desc != null) {
          capabilities = desc.getCapabilities();
        } else {
          wasNewDim = true;
          capabilities = columnCapabilities.get(dimension);
          if (capabilities == null) {
            capabilities = new ColumnCapabilitiesImpl();
            // For schemaless type discovery, assume everything is a String for now, can change later.
            capabilities.setType(ValueType.STRING);
            capabilities.setDictionaryEncoded(true);
            capabilities.setHasBitmapIndexes(true);
            columnCapabilities.put(dimension, capabilities);
          }
          DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dimension, capabilities, null);
          desc = addNewDimension(dimension, capabilities, handler);
        }
        DimensionHandler handler = desc.getHandler();
        DimensionIndexer indexer = desc.getIndexer();
        Object dimsKey = indexer.processRowValsToUnsortedEncodedKeyComponent(row.getRaw(dimension));

        // Set column capabilities as data is coming in
        if (!capabilities.hasMultipleValues() && dimsKey != null && handler.getLengthOfEncodedKeyComponent(dimsKey) > 1) {
          capabilities.setHasMultipleValues(true);
        }

        if (wasNewDim) {
          if (overflow == null) {
            overflow = Lists.newArrayList();
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
    return new TimeAndDims(Math.max(truncated, minTimestamp), dims, dimensionDescsList);
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

  public DimensionDesc getDimension(String dimension)
  {
    synchronized (dimensionDescs) {
      return dimensionDescs.get(dimension);
    }
  }

  public String getMetricType(String metric)
  {
    final MetricDesc metricDesc = metricDescs.get(metric);
    return metricDesc != null ? metricDesc.getType() : null;
  }

  public Class getMetricClass(String metric)
  {
    MetricDesc metricDesc = metricDescs.get(metric);
    switch (metricDesc.getCapabilities().getType()) {
      case COMPLEX:
        return ComplexMetrics.getSerdeForType(metricDesc.getType()).getObjectStrategy().getClazz();
      case FLOAT:
        return Float.TYPE;
      case LONG:
        return Long.TYPE;
      case STRING:
        return String.class;
    }
    return null;
  }

  public Interval getInterval()
  {
    return new Interval(minTimestamp, isEmpty() ? minTimestamp : gran.increment(new DateTime(getMaxTimeMillis())).getMillis());
  }

  public DateTime getMinTime()
  {
    return isEmpty() ? null : new DateTime(getMinTimeMillis());
  }

  public DateTime getMaxTime()
  {
    return isEmpty() ? null : new DateTime(getMaxTimeMillis());
  }

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

  private ColumnCapabilitiesImpl makeCapabilitesFromValueType(ValueType type)
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
    capabilities.setDictionaryEncoded(type == ValueType.STRING);
    capabilities.setHasBitmapIndexes(type == ValueType.STRING);
    capabilities.setType(type);
    return capabilities;
  }

  /**
   * Currently called to initialize IncrementalIndex dimension order during index creation
   * Index dimension ordering could be changed to initialize from DimensionsSpec after resolution of
   * https://github.com/druid-io/druid/issues/2011
   */
  public void loadDimensionIterable(Iterable<String> oldDimensionOrder, Map<String, ColumnCapabilitiesImpl> oldColumnCapabilities)
  {
    synchronized (dimensionDescs) {
      if (!dimensionDescs.isEmpty()) {
        throw new ISE("Cannot load dimension order when existing order[%s] is not empty.", dimensionDescs.keySet());
      }
      for (String dim : oldDimensionOrder) {
        if (dimensionDescs.get(dim) == null) {
          ColumnCapabilitiesImpl capabilities = oldColumnCapabilities.get(dim);
          columnCapabilities.put(dim, capabilities);
          DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
          addNewDimension(dim, capabilities, handler);
        }
      }
    }
  }

  @GuardedBy("dimensionDescs")
  private DimensionDesc addNewDimension(String dim, ColumnCapabilitiesImpl capabilities, DimensionHandler handler)
  {
    DimensionDesc desc = new DimensionDesc(dimensionDescs.size(), dim, capabilities, handler);
    dimensionDescs.put(dim, desc);
    dimensionDescsList.add(desc);
    return desc;
  }

  public List<String> getMetricNames()
  {
    return ImmutableList.copyOf(metricDescs.keySet());
  }

  public List<MetricDesc> getMetrics()
  {
    return ImmutableList.copyOf(metricDescs.values());
  }

  public Integer getMetricIndex(String metricName)
  {
    MetricDesc metSpec = metricDescs.get(metricName);
    return metSpec == null ? null : metSpec.getIndex();
  }

  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
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

  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    Map<String, DimensionHandler> handlers = Maps.newLinkedHashMap();
    for (DimensionDesc desc : dimensionDescsList) {
      handlers.put(desc.getName(), desc.getHandler());
    }
    return handlers;
  }

  @Override
  public Iterator<Row> iterator()
  {
    return iterableWithPostAggregations(null, false).iterator();
  }

  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        final List<DimensionDesc> dimensions = getDimensions();

        return Iterators.transform(
            getFacts().iterator(descending),
            timeAndDims -> {
              final int rowOffset = timeAndDims.getRowIndex();

              Object[] theDims = timeAndDims.getDims();

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
                Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(dim, DimensionIndexer.LIST);
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

              return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
            }
        );
      }
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
    private final ColumnCapabilitiesImpl capabilities;
    private final DimensionHandler handler;
    private final DimensionIndexer indexer;

    public DimensionDesc(int index, String name, ColumnCapabilitiesImpl capabilities, DimensionHandler handler)
    {
      this.index = index;
      this.name = name;
      this.capabilities = capabilities;
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

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
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
    private final ColumnCapabilitiesImpl capabilities;

    public MetricDesc(int index, AggregatorFactory factory)
    {
      this.index = index;
      this.name = factory.getName();

      String typeInfo = factory.getTypeName();
      this.capabilities = new ColumnCapabilitiesImpl();
      if (typeInfo.equalsIgnoreCase("float")) {
        capabilities.setType(ValueType.FLOAT);
        this.type = typeInfo;
      } else if (typeInfo.equalsIgnoreCase("long")) {
        capabilities.setType(ValueType.LONG);
        this.type = typeInfo;
      } else {
        capabilities.setType(ValueType.COMPLEX);
        this.type = ComplexMetrics.getSerdeForType(typeInfo).getTypeName();
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

    public ColumnCapabilitiesImpl getCapabilities()
    {
      return capabilities;
    }
  }

  public static final class TimeAndDims
  {
    public static final int EMPTY_ROW_INDEX = -1;

    private final long timestamp;
    private final Object[] dims;
    private final List<DimensionDesc> dimensionDescsList;

    /**
     * rowIndex is not checked in {@link #equals} and {@link #hashCode} on purpose. TimeAndDims acts as a Map key
     * and "entry" object (rowIndex is the "value") at the same time. This is done to reduce object indirection and
     * improve locality, and avoid boxing of rowIndex as Integer, when stored in JDK collection:
     * {@link RollupFactsHolder} needs concurrent collections, that are not present in fastutil.
     */
    private int rowIndex;

    TimeAndDims(
        long timestamp,
        Object[] dims,
        List<DimensionDesc> dimensionDescsList
    )
    {
      this(timestamp, dims, dimensionDescsList, EMPTY_ROW_INDEX);
    }

    TimeAndDims(
        long timestamp,
        Object[] dims,
        List<DimensionDesc> dimensionDescsList,
        int rowIndex
    )
    {
      this.timestamp = timestamp;
      this.dims = dims;
      this.dimensionDescsList = dimensionDescsList;
      this.rowIndex = rowIndex;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public Object[] getDims()
    {
      return dims;
    }

    public int getRowIndex()
    {
      return rowIndex;
    }

    private void setRowIndex(int rowIndex)
    {
      this.rowIndex = rowIndex;
    }

    @Override
    public String toString()
    {
      return "TimeAndDims{" +
             "timestamp=" + new DateTime(timestamp) +
             ", dims=" + Lists.transform(
          Arrays.asList(dims), new Function<Object, Object>()
          {
            @Override
            public Object apply(@Nullable Object input)
            {
              if (input == null || Array.getLength(input) == 0) {
                return Collections.singletonList("null");
              }
              return Collections.singletonList(input);
            }
          }
      ) + '}';
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

      TimeAndDims that = (TimeAndDims) o;

      if (timestamp != that.timestamp) {
        return false;
      }
      if (dims.length != that.dims.length) {
        return false;
      }
      for (int i = 0; i < dims.length; i++) {
        final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
        if (!indexer.checkUnsortedEncodedKeyComponentsEqual(dims[i], that.dims[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode()
    {
      int hash = (int) timestamp;
      for (int i = 0; i < dims.length; i++) {
        final DimensionIndexer indexer = dimensionDescsList.get(i).getIndexer();
        hash = 31 * hash + indexer.getUnsortedEncodedKeyComponentHashCode(dims[i]);
      }
      return hash;
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

  protected final Comparator<TimeAndDims> dimsComparator()
  {
    return new TimeAndDimsComp(dimensionDescsList);
  }

  @VisibleForTesting
  static final class TimeAndDimsComp implements Comparator<TimeAndDims>
  {
    private List<DimensionDesc> dimensionDescs;

    public TimeAndDimsComp(List<DimensionDesc> dimDescs)
    {
      this.dimensionDescs = dimDescs;
    }

    @Override
    public int compare(TimeAndDims lhs, TimeAndDims rhs)
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
     * {@code TimeAndDims#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int getPriorIndex(TimeAndDims key);

    long getMinTimeMillis();

    long getMaxTimeMillis();

    Iterator<TimeAndDims> iterator(boolean descending);

    Iterable<TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

    Iterable<TimeAndDims> keySet();

    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@code TimeAndDims#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int putIfAbsent(TimeAndDims key, int rowIndex);

    void clear();
  }

  static class RollupFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    // Can't use Set because we need to be able to get from collection
    private final ConcurrentMap<TimeAndDims, TimeAndDims> facts;
    private final List<DimensionDesc> dimensionDescsList;

    public RollupFactsHolder(boolean sortFacts, Comparator<TimeAndDims> timeAndDimsComparator, List<DimensionDesc> dimensionDescsList)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>(timeAndDimsComparator);
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int getPriorIndex(TimeAndDims key)
    {
      TimeAndDims timeAndDims = facts.get(key);
      return timeAndDims == null ? TimeAndDims.EMPTY_ROW_INDEX : timeAndDims.rowIndex;
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<TimeAndDims, TimeAndDims>) facts).firstKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<TimeAndDims, TimeAndDims>) facts).lastKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<TimeAndDims> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return ((ConcurrentNavigableMap<TimeAndDims, TimeAndDims>) facts).descendingMap().keySet().iterator();
      }
      return keySet().iterator();
    }

    @Override
    public Iterable<TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (!sortFacts) {
        throw new UnsupportedOperationException("can't get timeRange from unsorted facts data.");
      }
      TimeAndDims start = new TimeAndDims(timeStart, new Object[]{}, dimensionDescsList);
      TimeAndDims end = new TimeAndDims(timeEnd, new Object[]{}, dimensionDescsList);
      ConcurrentNavigableMap<TimeAndDims, TimeAndDims> subMap =
          ((ConcurrentNavigableMap<TimeAndDims, TimeAndDims>) facts).subMap(start, end);
      final Map<TimeAndDims, TimeAndDims> rangeMap = descending ? subMap.descendingMap() : subMap;
      return rangeMap.keySet();
    }

    @Override
    public Iterable<TimeAndDims> keySet()
    {
      return facts.keySet();
    }

    @Override
    public int putIfAbsent(TimeAndDims key, int rowIndex)
    {
      // setRowIndex() must be called before facts.putIfAbsent() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      TimeAndDims prev = facts.putIfAbsent(key, key);
      return prev == null ? TimeAndDims.EMPTY_ROW_INDEX : prev.rowIndex;
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
    private final ConcurrentMap<Long, Deque<TimeAndDims>> facts;

    public PlainFactsHolder(boolean sortFacts)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>();
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
    }

    @Override
    public int getPriorIndex(TimeAndDims key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return TimeAndDims.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<TimeAndDims>>) facts).firstKey();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<TimeAndDims>>) facts).lastKey();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<TimeAndDims> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return concat(((ConcurrentNavigableMap<Long, Deque<TimeAndDims>>) facts)
                .descendingMap().values(), true).iterator();
      }
      return concat(facts.values(), false).iterator();
    }

    @Override
    public Iterable<TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      ConcurrentNavigableMap<Long, Deque<TimeAndDims>> subMap =
          ((ConcurrentNavigableMap<Long, Deque<TimeAndDims>>) facts).subMap(timeStart, timeEnd);
      final Map<Long, Deque<TimeAndDims>> rangeMap = descending ? subMap.descendingMap() : subMap;
      return concat(rangeMap.values(), descending);
    }

    private Iterable<TimeAndDims> concat(
        final Iterable<Deque<TimeAndDims>> iterable,
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

    @Override
    public Iterable<TimeAndDims> keySet()
    {
      return concat(facts.values(), false);
    }

    @Override
    public int putIfAbsent(TimeAndDims key, int rowIndex)
    {
      Long time = key.getTimestamp();
      Deque<TimeAndDims> rows = facts.get(time);
      if (rows == null) {
        facts.putIfAbsent(time, new ConcurrentLinkedDeque<>());
        // in race condition, rows may be put by other thread, so always get latest status from facts
        rows = facts.get(time);
      }
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      rows.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return TimeAndDims.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }
}
