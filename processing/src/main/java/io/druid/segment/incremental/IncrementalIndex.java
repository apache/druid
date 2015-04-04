/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Striped;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 */
public abstract class IncrementalIndex<AggregatorType> implements Iterable<Row>, Closeable
{
  private static final int ROW_SYNC_STRIPE_COUNT = 64;
  private static final int DIM_SYNC_STRIPE_COUNT = ROW_SYNC_STRIPE_COUNT;
  private volatile DateTime maxIngestedEventTime;

  protected static ColumnSelectorFactory makeColumnSelectorFactory(
      final AggregatorFactory agg,
      final ThreadLocal<InputRow> in,
      final boolean deserializeComplexMetrics
  )
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public LongColumnSelector makeLongColumnSelector(final String columnName)
      {
        if (columnName.equals(Column.TIME_COLUMN_NAME)) {
          return new LongColumnSelector()
          {
            @Override
            public long get()
            {
              return in.get().getTimestampFromEpoch();
            }
          };
        }
        return new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return in.get().getLongMetric(columnName);
          }
        };
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(final String columnName)
      {
        return new FloatColumnSelector()
        {
          @Override
          public float get()
          {
            return in.get().getFloatMetric(columnName);
          }
        };
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(final String column)
      {
        final String typeName = agg.getTypeName();

        final ObjectColumnSelector<Object> rawColumnSelector = new ObjectColumnSelector<Object>()
        {
          @Override
          public Class classOfObject()
          {
            return Object.class;
          }

          @Override
          public Object get()
          {
            return in.get().getRaw(column);
          }
        };

        if (!deserializeComplexMetrics) {
          return rawColumnSelector;
        } else {
          if (typeName.equals("float")) {
            return rawColumnSelector;
          }

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
      public DimensionSelector makeDimensionSelector(final String dimension, final ExtractionFn extractionFn)
      {
        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            final List<String> dimensionValues = in.get().getDimension(dimension);
            final ArrayList<Integer> vals = Lists.newArrayList();
            if (dimensionValues != null) {
              for (int i = 0; i < dimensionValues.size(); ++i) {
                vals.add(i);
              }
            }

            return new IndexedInts()
            {
              @Override
              public int size()
              {
                return vals.size();
              }

              @Override
              public int get(int index)
              {
                return vals.get(index);
              }

              @Override
              public Iterator<Integer> iterator()
              {
                return vals.iterator();
              }
            };
          }

          @Override
          public int getValueCardinality()
          {
            throw new UnsupportedOperationException("value cardinality is unknown in incremental index");
          }

          @Override
          public String lookupName(int id)
          {
            final String value = in.get().getDimension(dimension).get(id);
            return extractionFn == null ? value : extractionFn.apply(value);
          }

          @Override
          public int lookupId(String name)
          {
            if (extractionFn != null) {
              throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
            }
            return in.get().getDimension(dimension).indexOf(name);
          }
        };
      }
    };
  }

  private final long minTimestamp;
  private final QueryGranularity gran;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final AggregatorFactory[] metrics;
  private final Map<String, Integer> metricIndexes;
  private final Map<String, String> metricTypes;
  private final ImmutableList<String> metricNames;
  private final ConcurrentMap<String, Integer> dimensionOrder = new ConcurrentHashMap<>();
  private final AtomicInteger orderIncrementer = new AtomicInteger(0);
  private final AtomicInteger rowIncrementer = new AtomicInteger(0);
  private final List<AggregatorType> aggs;
  private final DimensionHolder dimValues;
  private final ConcurrentMap<String, ColumnCapabilitiesImpl> columnCapabilities = new ConcurrentHashMap<>();
  private final boolean deserializeComplexMetrics;
  private final Striped<Lock> dimensionSynchronizer = Striped.lazyWeakLock(DIM_SYNC_STRIPE_COUNT);
  private final Object[] rowSynchronizer = new Object[ROW_SYNC_STRIPE_COUNT]; // Stripe keys are set as integers [0,63]


  private final AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  private ThreadLocal<InputRow> in = new ThreadLocal<>();

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param incrementalIndexSchema    the schema to use for incremental index
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   */
  public IncrementalIndex(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = new CopyOnWriteArrayList<>();
    this.deserializeComplexMetrics = deserializeComplexMetrics;

    final ImmutableList.Builder<String> metricNamesBuilder = ImmutableList.builder();
    final ImmutableMap.Builder<String, Integer> metricIndexesBuilder = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> metricTypesBuilder = ImmutableMap.builder();
    this.aggs = initAggs(metrics, in, deserializeComplexMetrics);

    for (int i = 0; i < metrics.length; i++) {
      final String metricName = metrics[i].getName();
      metricNamesBuilder.add(metricName);
      metricIndexesBuilder.put(metricName, i);
      metricTypesBuilder.put(metricName, metrics[i].getTypeName());
    }
    metricNames = metricNamesBuilder.build();
    metricIndexes = metricIndexesBuilder.build();
    metricTypes = metricTypesBuilder.build();

    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = incrementalIndexSchema.getDimensionsSpec().getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }

    for (Map.Entry<String, String> entry : metricTypes.entrySet()) {
      ValueType type;
      if (entry.getValue().equalsIgnoreCase("float")) {
        type = ValueType.FLOAT;
      } else if (entry.getValue().equalsIgnoreCase("long")) {
        type = ValueType.LONG;
      } else {
        type = ValueType.COMPLEX;
      }
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      capabilities.setType(type);
      columnCapabilities.put(entry.getKey(), capabilities);
    }
    for (String dimension : incrementalIndexSchema.getDimensionsSpec().getDimensions()) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      capabilities.setType(ValueType.STRING);
      columnCapabilities.put(dimension, capabilities);
    }
    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      capabilities.setType(ValueType.STRING);
      capabilities.setHasSpatialIndexes(true);
      columnCapabilities.put(spatialDimension.getDimName(), capabilities);
    }
    for (int i = 0; i < rowSynchronizer.length; ++i) {
      rowSynchronizer[i] = new Object();
    }
    this.dimValues = new DimensionHolder();
  }

  public abstract ConcurrentNavigableMap<TimeAndDims, Integer> getFacts();

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract DimDim makeDimDim(String dimension);

  protected abstract List<AggregatorType> initAggs(
      AggregatorFactory[] metrics,
      ThreadLocal<InputRow> in,
      boolean deserializeComplexMetrics
  );

  protected abstract List<AggregatorType> getAggsForRow(int rowIndex);

  protected abstract Object getAggVal(AggregatorType agg, int rowIndex, int aggPosition);

  protected abstract float getMetricFloatValue(int rowIndex, int aggOffset);

  protected abstract long getMetricLongValue(int rowIndex, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowIndex, int aggOffset);

  @Override
  public void close()
  {
    IOException ex = null;
    for (String dimension : dimensionOrder.keySet()) {
      try {
        dimValues.get(dimension).close();
      }
      catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw Throwables.propagate(ex);
    }
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

  private final int dimSizer(List<String> rowDimensions)
  {
    return Math.max(rowDimensions.size(), orderIncrementer.get());
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one. This method is thread safe
   * <p/>
   * <p/>
   * Calls to add() are thread safe.
   * <p/>
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(InputRow row) throws IndexSizeExceededException
  {
    row = formatRow(row);
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    String[][] dims = null;
    final List<String> rowDimensions = row.getDimensions();

    for (String dimension : rowDimensions) {
      List<String> dimensionValues = row.getDimension(dimension);
      final Boolean raceWinner;
      if (dimensionOrder.containsKey(dimension)) {
        // Common case, don't even try to lock
        raceWinner = false;
      } else {
        final Lock dimLock = dimensionSynchronizer.get(dimension);
        dimLock.lock();
        try {
          // If race is won, populate values, else quickly unlock and use "normal" race looser codepath
          raceWinner = !dimensionOrder.containsKey(dimension);
          if (raceWinner) {
            // we won a race, add a new dimension
            // Set column capabilities as data is coming in if it wasn't set in constructor
            ColumnCapabilitiesImpl capabilities = columnCapabilities.get(dimension);
            if (capabilities == null) {
              capabilities = new ColumnCapabilitiesImpl();
              capabilities.setType(ValueType.STRING);
              ColumnCapabilitiesImpl other = columnCapabilities.putIfAbsent(dimension, capabilities);
              if (other != null) {
                throw new ISE("Bad columnCapabilites state");
              }
            }
            if (dimensionValues.size() > 1) {
              capabilities.setHasMultipleValues(true);
            }

            final Integer orderID = orderIncrementer.getAndIncrement();
            if (null == dims) {
              dims = new String[dimSizer(rowDimensions)][];
            } else if (orderID >= dims.length) {
              dims = Arrays.copyOf(dims, dimSizer(rowDimensions));
            }
            dims[orderID] = canonizeDimValues(dimValues.addIfAbsent(dimension), dimensionValues);
            dimensionOrder.put(dimension, orderID);
          }
        }
        finally {
          dimLock.unlock();
        }
      }
      if (!raceWinner) {
        if (dimensionValues.size() > 1) {
          columnCapabilities.get(dimension).setHasMultipleValues(true);
        }
        final Integer orderID = dimensionOrder.get(dimension);
        if (null == dims) {
          dims = new String[dimSizer(rowDimensions)][];
        } else if (orderID >= dims.length) {
          dims = Arrays.copyOf(dims, dimSizer(rowDimensions));
        }
        dims[orderID] = canonizeDimValues(dimValues.get(dimension), dimensionValues);
      }
    }

    if (dims == null) {
      // If this row has no dimensions for some reason
      dims = new String[orderIncrementer.get()][];
    }

    final TimeAndDims key = new TimeAndDims(Math.max(gran.truncate(row.getTimestampFromEpoch()), minTimestamp), dims);


    // Now do the aggregation
    Integer rowIndex = getFacts().get(key);

    if (rowIndex == null) {
      // Off to the races! Same logic as dimensions previously
      synchronized (rowSynchronizer[getRowStripeKey(key)]) {
        rowIndex = getFacts().get(key);
        if (rowIndex == null) {
          if (!canAppendRow()) {
            throw new IndexSizeExceededException("Maximum number of rows reached");
          }
          List<AggregatorType> aggs = new ArrayList<>(
              Collections2.transform(
                  Arrays.<AggregatorFactory>asList(metrics),
                  new Function<AggregatorFactory, AggregatorType>()
                  {
                    @Override
                    public AggregatorType apply(AggregatorFactory input)
                    {
                      return getFactorizeFunction(input).apply(
                          makeColumnSelectorFactory(input, in, deserializeComplexMetrics)
                      );
                    }
                  }
              )
          );
          rowIndex = rowIncrementer.getAndIncrement();
          concurrentSet(rowIndex, aggs);
          initializeAggs(aggs, rowIndex);
          if (getFacts().put(key, rowIndex) != null) {
            throw new ISE("Corrupt fact state");
          }
          numEntries.incrementAndGet(); // needs to come last for race condition on isEmpty()
        }
      }
    }

    in.set(row);
    applyAggregators(rowIndex);
    in.set(null);

    updateMaxIngestedTime(row.getTimestamp());
    return numEntries.get();
  }

  private Integer getRowStripeKey(TimeAndDims timeAndDims)
  {
    return Math.abs(timeAndDims.hashCode()) % ROW_SYNC_STRIPE_COUNT;
  }

  public synchronized void updateMaxIngestedTime(DateTime eventTime)
  {
    if (maxIngestedEventTime == null || maxIngestedEventTime.isBefore(eventTime)) {
      maxIngestedEventTime = eventTime;
    }
  }

  protected abstract void initializeAggs(List<AggregatorType> aggs, Integer rowIndex);

  protected abstract Function<ColumnSelectorFactory, AggregatorType> getFactorizeFunction(AggregatorFactory agg);

  protected abstract void applyAggregators(Integer rowIndex);

  protected abstract List<AggregatorType> concurrentGet(int offset);

  protected abstract void concurrentSet(int offset, List<AggregatorType> value);

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
    return getFacts().firstKey().getTimestamp();
  }

  private long getMaxTimeMillis()
  {
    return getFacts().lastKey().getTimestamp();
  }

  private String[] canonizeDimValues(final DimDim dimLookup, final List<String> dimValues)
  {
    final String[] retVal = new String[dimValues.size()];

    int count = 0;
    for (String dimValue : dimValues) {
      String canonicalDimValue = dimLookup.intern(dimValue);
      if (!dimLookup.contains(canonicalDimValue)) {
        dimLookup.add(dimValue);
      }
      retVal[count++] = canonicalDimValue;
    }
    Arrays.sort(retVal);

    return retVal;
  }

  public List<AggregatorType> getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  protected DimensionHolder getDimValues()
  {
    return dimValues;
  }

  private final ReadWriteLock orderLock = new ReentrantReadWriteLock();
  private volatile List<String> orderdDims;

  private List<String> getOrderedDimensions()
  {
    orderLock.readLock().lock();
    try {
      if (orderdDims != null && orderdDims.size() == dimensionOrder.size()) {
        return orderdDims;
      }
    }
    finally {
      orderLock.readLock().unlock();
    }
    orderLock.writeLock().lock();
    try {
      // In case someone else sorted during the lock
      if (orderdDims != null && orderdDims.size() == dimensionOrder.size()) {
        return orderdDims;
      }
      List<String> retval = new ArrayList<>(dimensionOrder.keySet());
      Collections.sort(
          retval, new Ordering<String>()
          {
            @Override
            public int compare(String o1, String o2)
            {
              return Integer.compare(dimensionOrder.get(o1), dimensionOrder.get(o2));
            }
          }
      );
      orderdDims = retval;
      return retval;
    }
    finally {
      orderLock.writeLock().unlock();
    }
  }

  public List<String> getDimensions()
  {
    return getOrderedDimensions();
  }

  public String getMetricType(String metric)
  {
    return metricTypes.get(metric);
  }

  public Interval getInterval()
  {
    return new Interval(minTimestamp, isEmpty() ? minTimestamp : gran.next(getMaxTimeMillis()));
  }

  public DateTime getMinTime()
  {
    return isEmpty() ? null : new DateTime(getMinTimeMillis());
  }

  public DateTime getMaxTime()
  {
    return isEmpty() ? null : new DateTime(getMaxTimeMillis());
  }

  public DimDim getDimension(String dimension)
  {
    return isEmpty() ? null : dimValues.get(dimension);
  }

  public Integer getDimensionIndex(String dimension)
  {
    return dimensionOrder.get(dimension);
  }

  public List<String> getMetricNames()
  {
    return metricNames;
  }

  public Integer getMetricIndex(String metricName)
  {
    return metricIndexes.get(metricName);
  }

  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  public ConcurrentNavigableMap<TimeAndDims, Integer> getSubMap(TimeAndDims start, TimeAndDims end)
  {
    return getFacts().subMap(start, end);
  }

  @Override
  public Iterator<Row> iterator()
  {
    return iterableWithPostAggregations(null).iterator();
  }

  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        return Iterators.transform(
            Iterators.filter(
                getFacts().entrySet().iterator(),
                new Predicate<Map.Entry<TimeAndDims, Integer>>()
                {
                  @Override
                  public boolean apply(
                      @Nullable Map.Entry<TimeAndDims, Integer> input
                  )
                  {
                    return input.getValue() != null && input.getValue() >= 0;
                  }
                }
            ),
            new Function<Map.Entry<TimeAndDims, Integer>, Row>()
            {
              @Override
              public Row apply(final Map.Entry<TimeAndDims, Integer> input)
              {
                List<String> dimensions = getOrderedDimensions();
                final TimeAndDims timeAndDims = input.getKey();
                final int rowIndex = input.getValue();

                String[][] theDims = timeAndDims.getDims();

                Map<String, Object> theVals = Maps.newLinkedHashMap();
                for (int i = 0; i < theDims.length; ++i) {
                  String[] dim = theDims[i];
                  if (dim != null && dim.length != 0) {
                    theVals.put(dimensions.get(i), dim.length == 1 ? dim[0] : Arrays.asList(dim));
                  }
                  else {
                    theVals.put(dimensions.get(i), null);
                  }
                }

                List<AggregatorType> aggs = getAggsForRow(rowIndex);
                for (int i = 0; i < aggs.size(); ++i) {
                  theVals.put(metrics[i].getName(), getAggVal(aggs.get(i), rowIndex, i));
                }

                if (postAggs != null) {
                  for (PostAggregator postAgg : postAggs) {
                    theVals.put(postAgg.getName(), postAgg.compute(theVals));
                  }
                }

                return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
              }
            }
        );
      }
    };
  }

  public DateTime getMaxIngestedEventTime()
  {
    return maxIngestedEventTime;
  }

  class DimensionHolder
  {
    private final ConcurrentMap<String, DimDim> dimensions = new ConcurrentHashMap<>();

    DimDim addIfAbsent(String dimension)
    {
      DimDim holder = get(dimension);
      if (holder == null) {
        holder = makeDimDim(dimension);
        DimDim maybeConcurrent = dimensions.putIfAbsent(dimension, holder);
        if (null != maybeConcurrent) {
          // Make sure to close out any resources that were used
          try {
            maybeConcurrent.close();
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
          holder = maybeConcurrent;
        }
      }
      return holder;
    }

    DimDim get(String dimension)
    {
      return dimensions.get(dimension);
    }
  }

  static interface DimDim extends Closeable
  {
    public String intern(String value);

    public int getId(String value);

    public String getValue(int id);

    public boolean contains(String value);

    public int size();

    public int add(String value);

    public int getSortedId(String value);

    public String getSortedValue(int index);

    public void sort();

    public boolean compareCannonicalValues(String s1, String s2);
  }

  public static class TimeAndDims implements Comparable<TimeAndDims>
  {
    private final Long timestamp;
    private final String[][] dims;

    TimeAndDims(
        long timestamp,
        String[][] dims
    )
    {
      this.timestamp = timestamp;
      this.dims = dims;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public String[][] getDims()
    {
      return dims;
    }

    @Override
    public int compareTo(TimeAndDims rhs)
    {
      int retVal = Longs.compare(timestamp, rhs.timestamp);

      if (retVal == 0) {
        retVal = Ints.compare(dims.length, rhs.dims.length);
      }

      int index = 0;
      while (retVal == 0 && index < dims.length) {
        final String[] lhsVals = dims[index];
        final String[] rhsVals = rhs.dims[index];

        if (lhsVals == rhsVals) {
          ++index;
          continue;
        }
        if (lhsVals == null) {
          return -1;
        }

        if (rhsVals == null) {
          return 1;
        }

        retVal = Ints.compare(lhsVals.length, rhsVals.length);

        int valsIndex = 0;
        while (retVal == 0 && valsIndex < lhsVals.length) {
          final String lVal = lhsVals[valsIndex];
          final String rVal = rhsVals[valsIndex];
          if (lVal == rVal) {
            ++valsIndex;
            continue;
          }
          if (lVal == null) {
            return -1;
          }
          if (rVal == null) {
            return 1;
          }
          retVal = lVal.compareTo(rVal);
          ++valsIndex;
        }
        ++index;
      }

      return retVal;
    }

    @Override
    public String toString()
    {
      return "TimeAndDims{" +
             "timestamp=" + new DateTime(timestamp) +
             ", dims=" + Lists.transform(
          Arrays.asList(dims), new Function<String[], Object>()
          {
            @Override
            public Object apply(@Nullable String[] input)
            {
              if (input == null || input.length == 0) {
                return Arrays.asList("null");
              }
              return Arrays.asList(input);
            }
          }
      ) + '}';
    }

    @Override
    public boolean equals(final Object other)
    {
      if (other == null) {
        return false;
      }
      if (!(other instanceof TimeAndDims)) {
        return false;
      }
      return this.compareTo((TimeAndDims) other) == 0;
    }

    @Override
    public int hashCode()
    {
      // Since timestamps are typically stepped by a regular value, this scrambles the results pretty well
      // See IncrementalIndexHashAssumptionsTest for a distribution of results
      Long result = (long) String.format("ScrambleMeSoftly%d", timestamp).hashCode();
      // Instead of using builtin hash stuff, this provides us with a bit better fine grained control
      for (String[] strings : dims) {
        if (strings != null) {
          for (String string : strings) {
            if (string != null) {
              result = ((result * 31) + string.hashCode()) ^ (result >>> 32);
            }
          }
        }
      }
      return result.intValue();
    }
  }
}
