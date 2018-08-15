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
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.AbstractIndex;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.Metadata;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

  protected final long minTimestamp;
  private final Granularity gran;
  private final boolean rollup;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  protected final boolean reportParseExceptions;
  private final Metadata metadata;

  private final Map<String, DimensionDesc> dimensionDescs;
  protected final List<DimensionDesc> dimensionDescsList;
  protected final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  protected final AtomicInteger numEntries = new AtomicInteger();
  protected final AtomicLong bytesInMemory = new AtomicLong();
  protected final int maxRowCount;
  protected String outOfRowsReason = null;

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
   * @param reportParseExceptions     flag whether or not to report ParseExceptions that occur while extracting values
   *                                  from input rows
   * @param maxRowCount               max number of rows
   */
  protected IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean reportParseExceptions,
      final int maxRowCount
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.rollup = incrementalIndexSchema.isRollup();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.reportParseExceptions = reportParseExceptions;
    this.maxRowCount = maxRowCount;

    this.columnCapabilities = Maps.newHashMap();
    this.metadata = new Metadata(
        null,
        AggsManager.getCombiningAggregators(incrementalIndexSchema.getMetrics()),
        incrementalIndexSchema.getTimestampSpec(),
        this.gran,
        this.rollup
    );

    DimensionsSpec dimensionsSpec = incrementalIndexSchema.getDimensionsSpec();
    this.dimensionDescs = Maps.newLinkedHashMap();

    this.dimensionDescsList = new ArrayList<>();
    for (DimensionSchema dimSchema : dimensionsSpec.getDimensions()) {
      ValueType type = TYPE_MAP.get(dimSchema.getValueType());
      String dimName = dimSchema.getName();
      ColumnCapabilitiesImpl capabilities = makeCapabilitiesFromValueType(type);
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
    private long maxBytesInMemory;

    public Builder()
    {
      incrementalIndexSchema = null;
      deserializeComplexMetrics = true;
      reportParseExceptions = true;
      concurrentEventAdd = false;
      sortFacts = true;
      maxRowCount = 0;
      maxBytesInMemory = 0;
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
      return setSimpleTestingIndexSchema(null, metrics);
    }


    /**
     * A helper method to set a simple index schema with controllable metrics and rollup, and default values for the
     * other parameters. Note that this method is normally used for testing and benchmarking; it is unlikely that you
     * would use it in production settings.
     *
     * @param metrics variable array of {@link AggregatorFactory} metrics
     *
     * @return this
     */
    @VisibleForTesting
    public Builder setSimpleTestingIndexSchema(@Nullable Boolean rollup, final AggregatorFactory... metrics)
    {
      IncrementalIndexSchema.Builder builder = new IncrementalIndexSchema.Builder().withMetrics(metrics);
      this.incrementalIndexSchema = rollup != null ? builder.withRollup(rollup).build() : builder.build();
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

    //maxBytesInMemory only applies to OnHeapIncrementalIndex
    public Builder setMaxBytesInMemory(final long maxBytesInMemory)
    {
      this.maxBytesInMemory = maxBytesInMemory;
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
          maxRowCount,
          maxBytesInMemory
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

      return new OakIncrementalIndex(
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

  public abstract int getLastRowIndex();

  protected abstract AggregatorType[] getAggsForRow(IncrementalIndexRow incrementalIndexRow);

  protected abstract Object getAggVal(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  protected abstract float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  protected abstract long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  protected abstract Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  protected abstract double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  protected abstract boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex);

  static class IncrementalIndexRowResult
  {
    private IncrementalIndexRow incrementalIndexRow;
    private List<String> parseExceptionMessages;

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
    private int rowCount;
    private final long bytesInMemory;
    private List<String> parseExceptionMessages;

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

  public abstract Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

  public abstract Iterable<IncrementalIndexRow> keySet();

  public abstract Iterable<IncrementalIndexRow> persistIterable();

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
  public IncrementalIndexAddResult add(InputRow row) throws IndexSizeExceededException
  {
    return add(row, false);
  }

  public abstract IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException;

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
        Object dimsKey = null;
        try {
          dimsKey = indexer.processRowValsToUnsortedEncodedKeyComponent(
              row.getRaw(dimension),
              true
          );
        }
        catch (ParseException pe) {
          parseExceptionMessages.add(pe.getMessage());
        }
        dimsKeySize += indexer.estimateEncodedKeyComponentSize(dimsKey);
        // Set column capabilities as data is coming in
        if (!capabilities.hasMultipleValues() &&
            dimsKey != null &&
            handler.getLengthOfEncodedKeyComponent(dimsKey) > 1) {
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
    IncrementalIndexRow incrementalIndexRow = IncrementalIndexRow.createTimeAndDimswithDimsKeySize(
        Math.max(truncated, minTimestamp),
        dims,
        dimensionDescsList,
        dimsKeySize
    );
    return new IncrementalIndexRowResult(incrementalIndexRow, parseExceptionMessages);
  }

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
    ParseException pe = new ParseException(
        "Found unparseable columns in row: [%s], exceptions: [%s]",
        row,
        stringBuilder.toString()
    );
    pe.setFromPartiallyValidRow(true);
    return pe;
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

  public long getBytesInMemory()
  {
    return bytesInMemory.get();
  }

  protected abstract long getMinTimeMillis();

  protected abstract long getMaxTimeMillis();

  public abstract AggregatorType[] getAggs();

  public abstract AggregatorFactory[] getMetricAggs();

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
  public abstract String getMetricType(String metric);

  public abstract ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry);

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

  private ColumnCapabilitiesImpl makeCapabilitiesFromValueType(ValueType type)
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

  public abstract List<String> getMetricNames();

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

  public static boolean allNull(Object[] dims, int startPosition)
  {
    for (int i = startPosition; i < dims.length; i++) {
      if (dims[i] != null) {
        return false;
      }
    }
    return true;
  }
}
