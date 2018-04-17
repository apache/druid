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
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.collections.NonBlockingPool;
import io.druid.common.config.NullHandling;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.AbstractIndex;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.Metadata;
import io.druid.segment.NilColumnValueSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.StorageAdapter;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class IncrementalIndex<AggregatorType> extends AbstractIndex implements Iterable<Row>, Closeable
{
  protected volatile DateTime maxIngestedEventTime;

  // Used to discover ValueType based on the class of values in a row
  // Also used to convert between the duplicate ValueType enums in DimensionSchema (druid-api) and main druid.
  public static final Map<Object, ValueType> TYPE_MAP = ImmutableMap.<Object, ValueType>builder()
      .put(Long.class, ValueType.LONG)
      .put(Double.class, ValueType.DOUBLE)
      .put(Float.class, ValueType.FLOAT)
      .put(String.class, ValueType.STRING)
      .put(DimensionSchema.ValueType.LONG, ValueType.LONG)
      .put(DimensionSchema.ValueType.FLOAT, ValueType.FLOAT)
      .put(DimensionSchema.ValueType.STRING, ValueType.STRING)
      .put(DimensionSchema.ValueType.DOUBLE, ValueType.DOUBLE)
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
      public ColumnValueSelector<?> makeColumnValueSelector(final String column)
      {
        final String typeName = agg.getTypeName();
        boolean isComplexMetric =
            GuavaUtils.getEnumIfPresent(ValueType.class, StringUtils.toUpperCase(typeName)) == null ||
            typeName.equalsIgnoreCase(ValueType.COMPLEX.name());
        if (!isComplexMetric || !deserializeComplexMetrics) {
          return baseSelectorFactory.makeColumnValueSelector(column);
        } else {
          final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
          if (serde == null) {
            throw new ISE("Don't know how to handle type[%s]", typeName);
          }

          final ComplexMetricExtractor extractor = serde.getExtractor();
          return new ColumnValueSelector()
          {
            @Override
            public boolean isNull()
            {
              return in.get().getMetric(column) == null;
            }

            @Override
            public long getLong()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).longValue();
            }

            @Override
            public float getFloat()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).floatValue();
            }

            @Override
            public double getDouble()
            {
              Number metric = in.get().getMetric(column);
              assert NullHandling.replaceWithDefault() || metric != null;
              return DimensionHandlerUtils.nullToZero(metric).doubleValue();
            }

            @Override
            public Class classOfObject()
            {
              return extractor.extractedClass();
            }

            @Override
            public Object getObject()
            {
              return extractor.extractValue(in.get(), column);
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              inspector.visit("in", in);
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

  protected final long minTimestamp;
  private final Granularity gran;
  private final boolean rollup;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final VirtualColumns virtualColumns;
  protected final AggregatorFactory[] metrics;
  private final AggregatorType[] aggs;
  protected final boolean deserializeComplexMetrics;
  protected final boolean reportParseExceptions;
  private final Metadata metadata;

  private final Map<String, MetricDesc> metricDescs;

  private final Map<String, DimensionDesc> dimensionDescs;
  protected final List<DimensionDesc> dimensionDescsList;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  protected final AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  protected final ThreadLocal<InputRow> in = new ThreadLocal<>();
  protected final Supplier<InputRow> rowSupplier = in::get;

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
   * @param concurrentEventAdd        flag whether or not adding of input rows should be thread-safe
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
      capabilities.setHasBitmapIndexes(dimSchema.hasBitmapIndex());

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

    public IncrementalIndex buildOffheapOak()
    {
      if (maxRowCount <= 0) {
        throw new IllegalArgumentException("Invalid max row count: " + maxRowCount);
      }

      return new OffheapOakIncrementalIndex(
              Objects.requireNonNull(incrementalIndexSchema, "incrementalIndexSchema is null"),
              deserializeComplexMetrics,
              reportParseExceptions,
              concurrentEventAdd,
              maxRowCount
      );
    }
  }

  public boolean isRollup()
  {
    return rollup;
  }

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract AggregatorType[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  );

  public abstract int getLastRowIndex();

  protected abstract AggregatorType[] getAggsForRow(TimeAndDims timeAndDims);

  protected abstract Object getAggVal(AggregatorType agg, TimeAndDims timeAndDims, int aggPosition);

  protected abstract float getMetricFloatValue(TimeAndDims timeAndDims, int aggOffset);

  protected abstract long getMetricLongValue(TimeAndDims timeAndDims, int aggOffset);

  protected abstract Object getMetricObjectValue(TimeAndDims timeAndDims, int aggOffset);

  protected abstract double getMetricDoubleValue(TimeAndDims timeAndDims, int aggOffset);

  protected abstract boolean isNull(TimeAndDims timeAndDims, int aggOffset);

  public abstract Iterable<TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

  public abstract Iterable<TimeAndDims> keySet();

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
    return add(row, false);
  }

  public abstract int add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException;

  @VisibleForTesting
  TimeAndDims toTimeAndDims(InputRow row)
  {
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, DateTimes.utc(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();

    Object[] dims;
    List<Object> overflow = null;
    synchronized (dimensionDescs) {
      dims = new Object[dimensionDescs.size()];
      for (String dimension : rowDimensions) {
        if (Strings.isNullOrEmpty(dimension)) {
          continue;
        }
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
        Object dimsKey = indexer.processRowValsToUnsortedEncodedKeyComponent(
            row.getRaw(dimension),
            reportParseExceptions
        );

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

  protected synchronized void updateMaxIngestedTime(DateTime eventTime)
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

  protected abstract long getMinTimeMillis();

  protected abstract long getMaxTimeMillis();

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

  @Nullable
  public String getMetricType(String metric)
  {
    final MetricDesc metricDesc = metricDescs.get(metric);
    return metricDesc != null ? metricDesc.getType() : null;
  }

  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, TimeAndDimsHolder currEntry)
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

  public abstract Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggs, boolean descending);

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
      } else if (typeInfo.equalsIgnoreCase("double")) {
        capabilities.setType(ValueType.DOUBLE);
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
     * needs concurrent collections, that are not present in fastutil.
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

    public void setRowIndex(int rowIndex)
    {
      this.rowIndex = rowIndex;
    }

    @Override
    public String toString()
    {
      return "TimeAndDims{" +
             "timestamp=" + DateTimes.utc(timestamp) +
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

  private class LongMetricColumnSelector implements LongColumnSelector
  {
    private final TimeAndDimsHolder currEntry;
    private final int metricIndex;

    public LongMetricColumnSelector(TimeAndDimsHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public long getLong()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricLongValue(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get(), metricIndex);
    }
  }

  private class ObjectMetricColumnSelector extends ObjectColumnSelector
  {
    private final TimeAndDimsHolder currEntry;
    private final int metricIndex;
    private Class classOfObject;

    public ObjectMetricColumnSelector(
        MetricDesc metricDesc,
        TimeAndDimsHolder currEntry,
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
      return getMetricObjectValue(currEntry.get(), metricIndex);
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
    private final TimeAndDimsHolder currEntry;
    private final int metricIndex;

    public FloatMetricColumnSelector(TimeAndDimsHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public float getFloat()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricFloatValue(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get(), metricIndex);
    }
  }

  private class DoubleMetricColumnSelector implements DoubleColumnSelector
  {
    private final TimeAndDimsHolder currEntry;
    private final int metricIndex;

    public DoubleMetricColumnSelector(TimeAndDimsHolder currEntry, int metricIndex)
    {
      this.currEntry = currEntry;
      this.metricIndex = metricIndex;
    }

    @Override
    public double getDouble()
    {
      assert NullHandling.replaceWithDefault() || !isNull();
      return getMetricDoubleValue(currEntry.get(), metricIndex);
    }

    @Override
    public boolean isNull()
    {
      return IncrementalIndex.this.isNull(currEntry.get(), metricIndex);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("index", IncrementalIndex.this);
    }
  }
}
