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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Nullable;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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
import com.metamx.druid.index.v1.serde.ComplexMetricExtractor;
import com.metamx.druid.index.v1.serde.ComplexMetricSerde;
import com.metamx.druid.index.v1.serde.ComplexMetrics;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.processing.ComplexMetricSelector;
import com.metamx.druid.processing.FloatMetricSelector;
import com.metamx.druid.processing.MetricSelectorFactory;

/**
 */
public class IncrementalIndex implements Iterable<Row>
{
  private static final Logger log = new Logger(IncrementalIndex.class);

  private final long minTimestamp;
  private final QueryGranularity gran;
  final AggregatorFactory[] metrics;

  private final Map<String, Integer> metricIndexes;
  private final Map<String, String> metricTypes;
  final LinkedHashMap<String, Integer> dimensionOrder;
  final CopyOnWriteArrayList<String> dimensions;
  final DimensionHolder dimValues;
  final ConcurrentSkipListMap<TimeAndDims, Aggregator[]> facts;

  private volatile int numEntries = 0;

  // This is modified by the same thread.
  private InputRow in;

  public IncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics
  )
  {
    this.minTimestamp = minTimestamp;
    this.gran = gran;
    this.metrics = metrics;

    final ImmutableMap.Builder<String, Integer> metricIndexesBuilder = ImmutableMap.builder();
    final ImmutableMap.Builder<String, String> metricTypesBuilder = ImmutableMap.builder();
    for (int i = 0; i < metrics.length; i++) {
      metricIndexesBuilder.put(metrics[i].getName().toLowerCase(), i);
      metricTypesBuilder.put(metrics[i].getName().toLowerCase(), metrics[i].getTypeName());
    }
    metricIndexes = metricIndexesBuilder.build();
    metricTypes = metricTypesBuilder.build();

    this.dimensionOrder = Maps.newLinkedHashMap();
    this.dimensions = new CopyOnWriteArrayList<String>();
    this.dimValues = new DimensionHolder();

    this.facts = new ConcurrentSkipListMap<TimeAndDims, Aggregator[]>();
  }

  /**
   * Adds a new row.  The row might correspond with another row that already exists, in which case this will
   * update that row instead of inserting a new one.
   * <p/>
   * This is *not* thread-safe.  Calls to add() must be serialized externally
   *
   * @param row the row of data to add
   *
   * @return the number of rows in the data set after adding the InputRow
   */
  public int add(InputRow row)
  {
    if (row.getTimestampFromEpoch() < minTimestamp) {
      throw new IAE("Cannot add row[%s] because it is below the minTimestamp[%s]", row, new DateTime(minTimestamp));
    }

    final List<String> rowDimensions = row.getDimensions();
    String[][] dims = new String[dimensionOrder.size()][];

    List<String[]> overflow = null;
    for (String dimension : rowDimensions) {
      final Integer index = dimensionOrder.get(dimension);
      if (index == null) {
        dimensionOrder.put(dimension, dimensionOrder.size());
        dimensions.add(dimension);

        if (overflow == null) {
          overflow = Lists.newArrayList();
        }
        overflow.add(getDimVals(row, dimension));
      } else {
        dims[index] = getDimVals(row, dimension);
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
            new MetricSelectorFactory()
            {
              @Override
              public FloatMetricSelector makeFloatMetricSelector(final String metricName)
              {
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
              public ComplexMetricSelector makeComplexMetricSelector(final String metricName)
              {
                final String typeName = agg.getTypeName();
                final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

                if (serde == null) {
                  throw new ISE("Don't know how to handle type[%s]", typeName);
                }

                final ComplexMetricExtractor extractor = serde.getExtractor();

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

  private long getMinTimeMillis()
  {
    return facts.firstKey().getTimestamp();
  }

  private long getMaxTimeMillis()
  {
    return facts.lastKey().getTimestamp();
  }

  private String[] getDimVals(InputRow row, String dimension)
  {
    final DimDim dimLookup = dimValues.getOrAdd(dimension);
    final List<String> dimValues = row.getDimension(dimension);
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

  Integer getMetricIndex(String metricName)
  {
    return metricIndexes.get(metricName);
  }

  ConcurrentNavigableMap<TimeAndDims, Aggregator[]> getSubMap(TimeAndDims start, TimeAndDims end)
  {
    return facts.subMap(start, end);
  }

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

            return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
          }
        }
    );
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

    DimDim getOrAdd(String dimension)
    {
      DimDim holder = dimensions.get(dimension);
      if (holder == null) {
        holder = new DimDim();
        dimensions.put(dimension, holder);
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
      return poorMansInterning.get(value);
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
