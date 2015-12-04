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
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class IncrementalIndex<AggregatorType> implements Iterable<Row>, Closeable
{
  private volatile DateTime maxIngestedEventTime;

  public static ColumnSelectorFactory makeColumnSelectorFactory(
      final AggregatorFactory agg,
      final Supplier<InputRow> in,
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

              @Override
              public void close() throws IOException
              {

              }

              @Override
              public void fill(int index, int[] toFill)
              {
                throw new UnsupportedOperationException("fill not supported");
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
  private final LinkedHashMap<String, Integer> dimensionOrder;
  private final AggregatorType[] aggs;
  private final DimensionHolder dimValues;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final boolean deserializeComplexMetrics;

  protected final CopyOnWriteArrayList<String> dimensions;

  private volatile AtomicInteger numEntries = new AtomicInteger();

  // This is modified on add() in a critical section.
  private ThreadLocal<InputRow> in = new ThreadLocal<>();
  private Supplier<InputRow> rowSupplier = new Supplier<InputRow>()
  {
    @Override
    public InputRow get()
    {
      return in.get();
    }
  };

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
    this.aggs = initAggs(metrics, rowSupplier, deserializeComplexMetrics);

    for (int i = 0; i < metrics.length; i++) {
      final String metricName = metrics[i].getName();
      metricNamesBuilder.add(metricName);
      metricIndexesBuilder.put(metricName, i);
      metricTypesBuilder.put(metricName, metrics[i].getTypeName());
    }
    metricNames = metricNamesBuilder.build();
    metricIndexes = metricIndexesBuilder.build();
    metricTypes = metricTypesBuilder.build();

    this.dimensionOrder = Maps.newLinkedHashMap();
    this.dimensions = new CopyOnWriteArrayList<>(incrementalIndexSchema.getDimensionsSpec().getDimensions());
    // This should really be more generic
    List<SpatialDimensionSchema> spatialDimensions = incrementalIndexSchema.getDimensionsSpec().getSpatialDimensions();
    if (!spatialDimensions.isEmpty()) {
      this.rowTransformers.add(new SpatialDimensionRowTransformer(spatialDimensions));
    }

    this.columnCapabilities = Maps.newHashMap();
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
    this.dimValues = new DimensionHolder();
    for (String dimension : dimensions) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      capabilities.setType(ValueType.STRING);
      columnCapabilities.put(dimension, capabilities);
      dimensionOrder.put(dimension, dimensionOrder.size());
      dimValues.add(dimension);
    }
    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl();
      capabilities.setType(ValueType.STRING);
      capabilities.setHasSpatialIndexes(true);
      columnCapabilities.put(spatialDimension.getDimName(), capabilities);
    }
  }

  public abstract ConcurrentNavigableMap<TimeAndDims, Integer> getFacts();

  public abstract boolean canAppendRow();

  public abstract String getOutOfRowsReason();

  protected abstract DimDim makeDimDim(String dimension);

  protected abstract AggregatorType[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics
  );

  protected abstract Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException;

  protected abstract AggregatorType[] getAggsForRow(int rowOffset);

  protected abstract Object getAggVal(AggregatorType agg, int rowOffset, int aggPosition);

  protected abstract float getMetricFloatValue(int rowOffset, int aggOffset);

  protected abstract long getMetricLongValue(int rowOffset, int aggOffset);

  protected abstract Object getMetricObjectValue(int rowOffset, int aggOffset);

  @Override
  public void close()
  {
    // Nothing to close
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

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
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

    final List<String> rowDimensions = row.getDimensions();

    String[][] dims;
    List<String[]> overflow = null;
    synchronized (dimensionOrder) {
      dims = new String[dimensionOrder.size()][];
      for (String dimension : rowDimensions) {
        List<String> dimensionValues = row.getDimension(dimension);

        // Set column capabilities as data is coming in
        ColumnCapabilitiesImpl capabilities = columnCapabilities.get(dimension);
        if (capabilities == null) {
          capabilities = new ColumnCapabilitiesImpl();
          capabilities.setType(ValueType.STRING);
          columnCapabilities.put(dimension, capabilities);
        }
        if (dimensionValues.size() > 1) {
          capabilities.setHasMultipleValues(true);
        }

        Integer index = dimensionOrder.get(dimension);
        if (index == null) {
          dimensionOrder.put(dimension, dimensionOrder.size());
          dimensions.add(dimension);

          if (overflow == null) {
            overflow = Lists.newArrayList();
          }
          overflow.add(getDimVals(dimValues.add(dimension), dimensionValues));
        } else if (index > dims.length || dims[index] != null)  {
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
          dims[index] = getDimVals(dimValues.get(dimension), dimensionValues);
        }
      }
    }

    if (overflow != null) {
      // Merge overflow and non-overflow
      String[][] newDims = new String[dims.length + overflow.size()][];
      System.arraycopy(dims, 0, newDims, 0, dims.length);
      for (int i = 0; i < overflow.size(); ++i) {
        newDims[dims.length + i] = overflow.get(i);
      }
      dims = newDims;
    }

    final TimeAndDims key = new TimeAndDims(Math.max(gran.truncate(row.getTimestampFromEpoch()), minTimestamp), dims);
    final Integer rv = addToFacts(metrics, deserializeComplexMetrics, row, numEntries, key, in, rowSupplier);
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  public synchronized void updateMaxIngestedTime(DateTime eventTime)
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
    return getFacts().firstKey().getTimestamp();
  }

  private long getMaxTimeMillis()
  {
    return getFacts().lastKey().getTimestamp();
  }

  private String[] getDimVals(final DimDim dimLookup, final List<String> dimValues)
  {
    final String[] retVal = new String[dimValues.size()];
    if (dimValues.size() == 0) {
      // NULL VALUE
      if (!dimLookup.contains(null)) {
        dimLookup.add(null);
      }
      return null;
    }
    int count = 0;
    for (String dimValue : dimValues) {
      String canonicalDimValue = dimLookup.get(dimValue);
      if (!dimLookup.contains(canonicalDimValue)) {
        dimLookup.add(dimValue);
      }
      retVal[count] = canonicalDimValue;
      count++;
    }
    Arrays.sort(retVal);

    return retVal;
  }

  public AggregatorType[] getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  public DimensionHolder getDimValues()
  {
    return dimValues;
  }

  public List<String> getDimensions()
  {
    return dimensions;
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
            getFacts().entrySet().iterator(),
            new Function<Map.Entry<TimeAndDims, Integer>, Row>()
            {
              @Override
              public Row apply(final Map.Entry<TimeAndDims, Integer> input)
              {
                final TimeAndDims timeAndDims = input.getKey();
                final int rowOffset = input.getValue();

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
    private final Map<String, DimDim> dimensions;

    DimensionHolder()
    {
      dimensions = Maps.newConcurrentMap();
    }

    DimDim add(String dimension)
    {
      DimDim holder = dimensions.get(dimension);
      if (holder == null) {
        holder = new NullValueConverterDimDim(makeDimDim(dimension));
        dimensions.put(dimension, holder);
      } else {
        throw new ISE("dimension[%s] already existed even though add() was called!?", dimension);
      }
      return holder;
    }

    DimDim get(String dimension)
    {
      return dimensions.get(dimension);
    }
  }

  static interface DimDim
  {
    public String get(String value);

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

  /**
   * implementation which converts null strings to empty strings and vice versa.
   */
  static class NullValueConverterDimDim implements DimDim
  {
    private final DimDim delegate;

    NullValueConverterDimDim(DimDim delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public String get(String value)
    {
      return delegate.get(Strings.nullToEmpty(value));
    }

    @Override
    public int getId(String value)
    {
      return delegate.getId(Strings.nullToEmpty(value));
    }

    @Override
    public String getValue(int id)
    {
      return Strings.emptyToNull(delegate.getValue(id));
    }

    @Override
    public boolean contains(String value)
    {
      return delegate.contains(Strings.nullToEmpty(value));
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public int add(String value)
    {
      return delegate.add(Strings.nullToEmpty(value));
    }

    @Override
    public int getSortedId(String value)
    {
      return delegate.getSortedId(Strings.nullToEmpty(value));
    }

    @Override
    public String getSortedValue(int index)
    {
      return Strings.emptyToNull(delegate.getSortedValue(index));
    }

    @Override
    public void sort()
    {
      delegate.sort();
    }

    @Override
    public boolean compareCannonicalValues(String s1, String s2)
    {
      return delegate.compareCannonicalValues(Strings.nullToEmpty(s1), Strings.nullToEmpty(s2));
    }
  }

  static class TimeAndDims implements Comparable<TimeAndDims>
  {
    private final long timestamp;
    private final String[][] dims;

    TimeAndDims(
        long timestamp,
        String[][] dims
    )
    {
      this.timestamp = timestamp;
      this.dims = dims;
    }

    long getTimestamp()
    {
      return timestamp;
    }

    String[][] getDims()
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
        String[] lhsVals = dims[index];
        String[] rhsVals = rhs.dims[index];

        if (lhsVals == null) {
          if (rhsVals == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsVals == null) {
          return 1;
        }

        retVal = Ints.compare(lhsVals.length, rhsVals.length);

        int valsIndex = 0;
        while (retVal == 0 && valsIndex < lhsVals.length) {
          retVal = lhsVals[valsIndex].compareTo(rhsVals[valsIndex]);
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
  }
}
