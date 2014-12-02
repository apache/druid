/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex implements IncrementalIndex
{
  private final long minTimestamp;
  private final QueryGranularity gran;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final AggregatorFactory[] metrics;
  private final Map<String, Integer> metricIndexes;
  private final Map<String, String> metricTypes;
  private final ImmutableList<String> metricNames;
  private final LinkedHashMap<String, Integer> dimensionOrder;
  protected final CopyOnWriteArrayList<String> dimensions;
  private final DimensionHolder dimValues;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;
  private final List<Aggregator[]> aggList;
  private volatile AtomicInteger numEntries = new AtomicInteger();
  // This is modified on add() in a critical section.
  private ThreadLocal<InputRow> in = new ThreadLocal<>();
  private final boolean deserializeComplexMetrics;

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param incrementalIndexSchema
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   */
  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics
  )
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.metrics = incrementalIndexSchema.getMetrics();
    this.rowTransformers = Lists.newCopyOnWriteArrayList();

    final ImmutableList.Builder<String> metricNamesBuilder = ImmutableList.builder();
    final ImmutableMap.Builder<String, Integer> metricIndexesBuilder = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> metricTypesBuilder = ImmutableMap.builder();
    this.aggList = Lists.newArrayList();

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
    this.dimensions = new CopyOnWriteArrayList<>();
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
    for (String dimension : dimensions) {
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
    this.dimValues = new DimensionHolder();
    this.facts = new ConcurrentSkipListMap<>();
    this.deserializeComplexMetrics = deserializeComplexMetrics;
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema
  )
  {
    this(incrementalIndexSchema, true);
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics
    );
  }

  @Override
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
  @Override
  public int add(InputRow row)
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
    Integer rowOffset;
    synchronized (this) {
      rowOffset = numEntries.get();
      final Integer prev = facts.putIfAbsent(key, rowOffset);
      if (prev != null) {
        rowOffset = prev;
      } else {
        Aggregator[] aggs = new Aggregator[metrics.length];
        for (int i = 0; i < metrics.length; i++) {
          final AggregatorFactory agg = metrics[i];
          aggs[i] = agg.factorize(
              new ColumnSelectorFactory()
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
                public DimensionSelector makeDimensionSelector(final String dimension)
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
                      return in.get().getDimension(dimension).get(id);
                    }

                    @Override
                    public int lookupId(String name)
                    {
                      return in.get().getDimension(dimension).indexOf(name);
                    }
                  };
                }
              }
          );
        }
        aggList.add(aggs);
        numEntries.incrementAndGet();
      }
    }
    in.set(row);
    Aggregator[] aggs = aggList.get(rowOffset);
    for (int i = 0; i < aggs.length; i++) {
      synchronized (aggs[i]) {
        aggs[i].aggregate();
      }
    }
    in.set(null);
    return numEntries.get();
  }

  @Override
  public boolean isEmpty()
  {
    return numEntries.get() == 0;
  }

  @Override
  public int size()
  {
    return numEntries.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return aggList.get(rowOffset)[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return aggList.get(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return aggList.get(rowOffset)[aggOffset].get();
  }

  private long getMinTimeMillis()
  {
    return facts.firstKey().getTimestamp();
  }

  private long getMaxTimeMillis()
  {
    return facts.lastKey().getTimestamp();
  }

  private String[] getDimVals(final DimDim dimLookup, final List<String> dimValues)
  {
    final String[] retVal = new String[dimValues.size()];

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

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
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

  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  public ConcurrentNavigableMap<TimeAndDims, Integer> getSubMap(TimeAndDims start, TimeAndDims end)
  {
    return facts.subMap(start, end);
  }

  @Override
  public Iterator<Row> iterator()
  {
    return iterableWithPostAggregations(null).iterator();
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        return Iterators.transform(
            facts.entrySet().iterator(),
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
                }
                Aggregator[] aggs = aggList.get(rowOffset);
                for (int i = 0; i < aggs.length; ++i) {
                  theVals.put(metrics[i].getName(), aggs[i].get());
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

  @Override
  public void close()
  {
    // Nothing to close
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
        holder = new DimDimImpl();
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

  private static class DimDimImpl implements DimDim
  {
    private final Map<String, Integer> falseIds;
    private final Map<Integer, String> falseIdsReverse;
    private volatile String[] sortedVals = null;
    final ConcurrentMap<String, String> poorMansInterning = Maps.newConcurrentMap();


    public DimDimImpl()
    {
      BiMap<String, Integer> biMap = Maps.synchronizedBiMap(HashBiMap.<String, Integer>create());
      falseIds = biMap;
      falseIdsReverse = biMap.inverse();
    }

    /**
     * Returns the interned String value to allow fast comparisons using `==` instead of `.equals()`
     *
     * @see io.druid.segment.incremental.IncrementalIndexStorageAdapter.EntryHolderValueMatcherFactory#makeValueMatcher(String, String)
     */
    public String get(String str)
    {
      String prev = poorMansInterning.putIfAbsent(str, str);
      return prev != null ? prev : str;
    }

    public int getId(String value)
    {
      if (value == null) {
        value = "";
      }
      final Integer id = falseIds.get(value);
      return id == null ? -1 : id;
    }

    public String getValue(int id)
    {
      return falseIdsReverse.get(id);
    }

    public boolean contains(String value)
    {
      return falseIds.containsKey(value);
    }

    public int size()
    {
      return falseIds.size();
    }

    public synchronized int add(String value)
    {
      int id = falseIds.size();
      falseIds.put(value, id);
      return id;
    }

    public int getSortedId(String value)
    {
      assertSorted();
      return Arrays.binarySearch(sortedVals, value);
    }

    public String getSortedValue(int index)
    {
      assertSorted();
      return sortedVals[index];
    }

    public void sort()
    {
      if (sortedVals == null) {
        sortedVals = new String[falseIds.size()];

        int index = 0;
        for (String value : falseIds.keySet()) {
          sortedVals[index++] = value;
        }
        Arrays.sort(sortedVals);
      }
    }

    private void assertSorted()
    {
      if (sortedVals == null) {
        throw new ISE("Call sort() before calling the getSorted* methods.");
      }
    }

    public boolean compareCannonicalValues(String s1, String s2)
    {
      return s1 == s2;
    }
  }
}
