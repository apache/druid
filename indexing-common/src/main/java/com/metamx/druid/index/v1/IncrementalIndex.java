/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.index.v1.serde.ComplexMetricExtractor;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.processing.ColumnSelectorFactory;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.FloatMetricSelector;
import com.metamx.druid.processing.ObjectColumnSelector;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 */
public class IncrementalIndex implements Iterable<Row>
{
  private static final Logger log = new Logger(IncrementalIndex.class);
  private static final Joiner JOINER = Joiner.on(",");

  private final long minTimestamp;
  private final QueryGranularity gran;
  private final AggregatorFactory[] metrics;

  private final Map<String, Integer> metricIndexes;
  private final Map<String, String> metricTypes;
  private final ImmutableList<String> metricNames;
  private final LinkedHashMap<String, Integer> dimensionOrder;
  private final CopyOnWriteArrayList<String> dimensions;
  private final List<SpatialDimensionSchema> spatialDimensions;
  private final SpatialDimensionRowFormatter spatialDimensionRowFormatter;
  private final DimensionHolder dimValues;
  private final ConcurrentSkipListMap<TimeAndDims, Aggregator[]> facts;

  private volatile int numEntries = 0;

  // This is modified on add() by a (hopefully) single thread.
  private InputRow in;

  public IncrementalIndex(IncrementalIndexSchema incrementalIndexSchema)
  {
    this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
    this.gran = incrementalIndexSchema.getGran();
    this.metrics = incrementalIndexSchema.getMetrics();

    final ImmutableList.Builder<String> metricNamesBuilder = ImmutableList.builder();
    final ImmutableMap.Builder<String, Integer> metricIndexesBuilder = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> metricTypesBuilder = ImmutableMap.builder();
    for (int i = 0; i < metrics.length; i++) {
      final String metricName = metrics[i].getName().toLowerCase();
      metricNamesBuilder.add(metricName);
      metricIndexesBuilder.put(metricName, i);
      metricTypesBuilder.put(metricName, metrics[i].getTypeName());
    }
    metricNames = metricNamesBuilder.build();
    metricIndexes = metricIndexesBuilder.build();
    metricTypes = metricTypesBuilder.build();

    this.dimensionOrder = Maps.newLinkedHashMap();
    this.dimensions = new CopyOnWriteArrayList<String>();
    int index = 0;
    for (String dim : incrementalIndexSchema.getDimensions()) {
      dimensionOrder.put(dim, index++);
      dimensions.add(dim);
    }
    this.spatialDimensions = incrementalIndexSchema.getSpatialDimensions();
    this.spatialDimensionRowFormatter = new SpatialDimensionRowFormatter(spatialDimensions);

    this.dimValues = new DimensionHolder();
    this.facts = new ConcurrentSkipListMap<TimeAndDims, Aggregator[]>();
  }

  public IncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build()
    );
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p/>
   * This is *not* thread-safe.  Calls to add() should always happen on the same thread.
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(InputRow row)
  {
    row = spatialDimensionRowFormatter.formatRow(row);

    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();
    String[][] dims = new String[dimensionOrder.size()][];

    List<String[]> overflow = null;
    for (String dimension : rowDimensions) {
      dimension = dimension.toLowerCase();
      List<String> dimensionValues = row.getDimension(dimension);

      final Integer index = dimensionOrder.get(dimension);
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

    if (overflow != null) {
      // Merge overflow and non-overflow
      String[][] newDims = new String[dims.length + overflow.size()][];
      System.arraycopy(dims, 0, newDims, 0, dims.length);
      for (int i = 0; i < overflow.size(); ++i) {
        newDims[dims.length + i] = overflow.get(i);
      }
      dims = newDims;
    }

    TimeAndDims key = new TimeAndDims(Math.max(gran.truncate(row.getTimestampFromEpoch()), minTimestamp), dims);

    in = row;
    Aggregator[] aggs = facts.get(key);
    if (aggs == null) {
      aggs = new Aggregator[metrics.length];

      for (int i = 0; i < metrics.length; ++i) {
        final AggregatorFactory agg = metrics[i];
        aggs[i] = agg.factorize(
            new ColumnSelectorFactory()
            {
              @Override
              public FloatMetricSelector makeFloatMetricSelector(String metric)
              {
                final String metricName = metric.toLowerCase();
                return new FloatMetricSelector()
                {
                  @Override
                  public float get()
                  {
                    return in.getFloatMetric(metricName);
                  }
                };
              }

              @Override
              public ComplexMetricSelector makeComplexMetricSelector(final String metric)
              {
                final String typeName = agg.getTypeName();

                final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

                if (serde == null) {
                  throw new ISE("Don't know how to handle type[%s]", typeName);
                }

                final ComplexMetricExtractor extractor = serde.getExtractor();
                final String metricName = metric.toLowerCase();

                return new ComplexMetricSelector()
                {
                  @Override
                  public Class classOfObject()
                  {
                    return extractor.extractedClass();
                  }

                  @Override
                  public Object get()
                  {
                    return extractor.extractValue(in, metricName);
                  }
                };
              }

              @Override
              public ObjectColumnSelector makeObjectColumnSelector(String column)
              {
                final String typeName = agg.getTypeName();
                final String columnName = column.toLowerCase();

                if (typeName.equals("float")) {
                  return new ObjectColumnSelector<Float>()
                  {
                    @Override
                    public Class classOfObject()
                    {
                      return Float.TYPE;
                    }

                    @Override
                    public Float get()
                    {
                      return in.getFloatMetric(columnName);
                    }
                  };
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
                    return extractor.extractValue(in, columnName);
                  }
                };
              }
            }
        );
      }

      facts.put(key, aggs);
      ++numEntries;
    }

    for (Aggregator agg : aggs) {
      agg.aggregate();
    }
    in = null;
    return numEntries;
  }

  public boolean isEmpty()
  {
    return numEntries == 0;
  }

  public int size()
  {
    return numEntries;
  }

  public long getMinTimeMillis()
  {
    return facts.firstKey().getTimestamp();
  }

  public long getMaxTimeMillis()
  {
    return facts.lastKey().getTimestamp();
  }

  private String[] getDimVals(final DimDim dimLookup, final List<String> dimValues)
  {
    final String[] retVal = new String[dimValues.size()];

    int count = 0;
    for (String dimValue : dimValues) {
      String canonicalDimValue = dimLookup.get(dimValue);
      if (canonicalDimValue == null) {
        canonicalDimValue = dimValue;
        dimLookup.add(dimValue);
      }

      retVal[count] = canonicalDimValue;
      count++;
    }
    Arrays.sort(retVal);

    return retVal;
  }

  public AggregatorFactory[] getMetricAggs()
  {
    return metrics;
  }

  public List<String> getDimensions()
  {
    return dimensions;
  }

  public List<SpatialDimensionSchema> getSpatialDimensions()
  {
    return spatialDimensions;
  }

  public SpatialDimensionRowFormatter getSpatialDimensionRowFormatter()
  {
    return spatialDimensionRowFormatter;
  }

  public String getMetricType(String metric)
  {
    return metricTypes.get(metric);
  }

  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  public QueryGranularity getGranularity()
  {
    return gran;
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

  DimDim getDimension(String dimension)
  {
    return isEmpty() ? null : dimValues.get(dimension);
  }

  Integer getDimensionIndex(String dimension)
  {
    return dimensionOrder.get(dimension);
  }

  List<String> getMetricNames()
  {
    return metricNames;
  }

  Integer getMetricIndex(String metricName)
  {
    return metricIndexes.get(metricName);
  }

  ConcurrentSkipListMap<TimeAndDims, Aggregator[]> getFacts()
  {
    return facts;
  }

  ConcurrentNavigableMap<TimeAndDims, Aggregator[]> getSubMap(TimeAndDims start, TimeAndDims end)
  {
    return facts.subMap(start, end);
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
            facts.entrySet().iterator(),
            new Function<Map.Entry<TimeAndDims, Aggregator[]>, Row>()
            {
              @Override
              public Row apply(final Map.Entry<TimeAndDims, Aggregator[]> input)
              {
                final TimeAndDims timeAndDims = input.getKey();
                final Aggregator[] aggregators = input.getValue();

                String[][] theDims = timeAndDims.getDims();

                Map<String, Object> theVals = Maps.newLinkedHashMap();
                for (int i = 0; i < theDims.length; ++i) {
                  String[] dim = theDims[i];
                  if (dim != null && dim.length != 0) {
                    theVals.put(dimensions.get(i), dim.length == 1 ? dim[0] : Arrays.asList(dim));
                  }
                }

                for (int i = 0; i < aggregators.length; ++i) {
                  theVals.put(metrics[i].getName(), aggregators[i].get());
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

  static class DimensionHolder
  {
    private final Map<String, DimDim> dimensions;

    DimensionHolder()
    {
      dimensions = Maps.newConcurrentMap();
    }

    void reset()
    {
      dimensions.clear();
    }

    DimDim add(String dimension)
    {
      DimDim holder = dimensions.get(dimension);
      if (holder == null) {
        holder = new DimDim();
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
      ) +
             '}';
    }
  }

  static class DimDim
  {
    private final Map<String, String> poorMansInterning = Maps.newConcurrentMap();
    private final Map<String, Integer> falseIds;
    private final Map<Integer, String> falseIdsReverse;

    private volatile String[] sortedVals = null;

    public DimDim()
    {
      BiMap<String, Integer> biMap = Maps.synchronizedBiMap(HashBiMap.<String, Integer>create());
      falseIds = biMap;
      falseIdsReverse = biMap.inverse();
    }

    public String get(String value)
    {
      return value == null ? null : poorMansInterning.get(value);
    }

    public int getId(String value)
    {
      return falseIds.get(value);
    }

    public String getValue(int id)
    {
      return falseIdsReverse.get(id);
    }

    public int size()
    {
      return poorMansInterning.size();
    }

    public Set<String> keySet()
    {
      return poorMansInterning.keySet();
    }

    public synchronized void add(String value)
    {
      poorMansInterning.put(value, value);
      falseIds.put(value, falseIds.size());
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
  }
}
