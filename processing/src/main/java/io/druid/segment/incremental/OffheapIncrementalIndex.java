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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
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
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class OffheapIncrementalIndex implements IncrementalIndex
{
  private final long minTimestamp;
  private final QueryGranularity gran;
  private final List<Function<InputRow, InputRow>> rowTransformers;
  private final AggregatorFactory[] metrics;
  private final Map<String, Integer> metricIndexes;
  private final Map<String, String> metricTypes;
  private final ImmutableList<String> metricNames;
  private final BufferAggregator[] aggs;
  private final int[] aggPositionOffsets;
  private final int totalAggSize;
  private final LinkedHashMap<String, Integer> dimensionOrder;
  protected final CopyOnWriteArrayList<String> dimensions;
  private final DimensionHolder dimValues;
  private final Map<String, ColumnCapabilitiesImpl> columnCapabilities;
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;
  private final ResourceHolder<ByteBuffer> bufferHolder;
  private final DB db;
  private final DB factsDb;
  private volatile AtomicInteger numEntries = new AtomicInteger();
  // This is modified on add() in a critical section.
  private ThreadLocal<InputRow> in = new ThreadLocal<>();

  /**
   * Setting deserializeComplexMetrics to false is necessary for intermediate aggregation such as groupBy that
   * should not deserialize input columns using ComplexMetricSerde for aggregators that return complex metrics.
   *
   * @param incrementalIndexSchema
   * @param bufferPool
   * @param deserializeComplexMetrics flag whether or not to call ComplexMetricExtractor.extractValue() on the input
   *                                  value for aggregators that return metrics other than float.
   */
  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      StupidPool<ByteBuffer> bufferPool,
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
    this.aggs = new BufferAggregator[metrics.length];
    this.aggPositionOffsets = new int[metrics.length];
    int currAggSize = 0;
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorizeBuffered(
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
      aggPositionOffsets[i] = currAggSize;
      currAggSize += agg.getMaxIntermediateSize();
      final String metricName = metrics[i].getName();
      metricNamesBuilder.add(metricName);
      metricIndexesBuilder.put(metricName, i);
      metricTypesBuilder.put(metricName, metrics[i].getTypeName());
    }
    metricNames = metricNamesBuilder.build();
    metricIndexes = metricIndexesBuilder.build();
    metricTypes = metricTypesBuilder.build();

    this.totalAggSize = currAggSize;

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
    this.bufferHolder = bufferPool.take();
    this.dimValues = new DimensionHolder();
    final DBMaker dbMaker = DBMaker.newMemoryDirectDB()
                                   .transactionDisable()
                                   .asyncWriteEnable()
                                   .cacheSoftRefEnable();
    factsDb = dbMaker.make();
    db = dbMaker.make();
    final TimeAndDimsSerializer timeAndDimsSerializer = new TimeAndDimsSerializer(this);
    this.facts = factsDb.createTreeMap("__facts" + UUID.randomUUID())
                        .keySerializer(timeAndDimsSerializer)
                        .comparator(timeAndDimsSerializer.getComparator())
                        .valueSerializer(Serializer.INTEGER)
                        .make();
  }


  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      StupidPool<ByteBuffer> bufferPool,
      boolean deserializeComplexMetrics
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        bufferPool,
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
      rowOffset = totalAggSize * numEntries.get();
      final Integer prev = facts.putIfAbsent(key, rowOffset);
      if (prev != null) {
        rowOffset = prev;
      } else {
        if (rowOffset + totalAggSize > bufferHolder.get().limit()) {
          facts.remove(key);
          throw new ISE("Buffer full, cannot add more rows! Current rowSize[%,d].", numEntries.get());
        }
        numEntries.incrementAndGet();
        for (int i = 0; i < aggs.length; i++) {
          aggs[i].init(bufferHolder.get(), getMetricPosition(rowOffset, i));
        }
      }
    }
    in.set(row);
    for (int i = 0; i < aggs.length; i++) {
      synchronized (aggs[i]) {
        aggs[i].aggregate(bufferHolder.get(), getMetricPosition(rowOffset, i));
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

  @Override
  public Interval getInterval()
  {
    return new Interval(minTimestamp, isEmpty() ? minTimestamp : gran.next(getMaxTimeMillis()));
  }

  @Override
  public DateTime getMinTime()
  {
    return isEmpty() ? null : new DateTime(getMinTimeMillis());
  }

  @Override
  public DateTime getMaxTime()
  {
    return isEmpty() ? null : new DateTime(getMaxTimeMillis());
  }

  @Override
  public DimDim getDimension(String dimension)
  {
    return isEmpty() ? null : dimValues.get(dimension);
  }

  @Override
  public Integer getDimensionIndex(String dimension)
  {
    return dimensionOrder.get(dimension);
  }

  @Override
  public List<String> getMetricNames()
  {
    return metricNames;
  }

  @Override
  public Integer getMetricIndex(String metricName)
  {
    return metricIndexes.get(metricName);
  }

  private int getMetricPosition(int rowOffset, int metricIndex)
  {
    return rowOffset + aggPositionOffsets[metricIndex];
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return aggs[aggOffset].getFloat(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return aggs[aggOffset].getLong(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return aggs[aggOffset].get(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return columnCapabilities.get(column);
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getSubMap(TimeAndDims start, TimeAndDims end)
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

                for (int i = 0; i < aggs.length; ++i) {
                  theVals.put(metrics[i].getName(), aggs[i].get(bufferHolder.get(), getMetricPosition(rowOffset, i)));
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
    try {
      bufferHolder.close();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
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
        holder = new OffheapDimDim(dimension);
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

  public static class TimeAndDimsSerializer extends BTreeKeySerializer<TimeAndDims> implements Serializable
  {
    private final TimeAndDimsComparator comparator;
    private final transient OffheapIncrementalIndex incrementalIndex;

    TimeAndDimsSerializer(OffheapIncrementalIndex incrementalIndex)
    {
      this.comparator = new TimeAndDimsComparator();
      this.incrementalIndex = incrementalIndex;
    }

    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException
    {
      for (int i = start; i < end; i++) {
        TimeAndDims timeAndDim = (TimeAndDims) keys[i];
        out.writeLong(timeAndDim.getTimestamp());
        out.writeInt(timeAndDim.getDims().length);
        int index = 0;
        for (String[] dims : timeAndDim.getDims()) {
          if (dims == null) {
            out.write(-1);
          } else {
            DimDim dimDim = incrementalIndex.getDimension(incrementalIndex.dimensions.get(index));
            out.writeInt(dims.length);
            for (String value : dims) {
              out.writeInt(dimDim.getId(value));
            }
          }
          index++;
        }
      }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException
    {
      Object[] ret = new Object[size];
      for (int i = start; i < end; i++) {
        final long timeStamp = in.readLong();
        final String[][] dims = new String[in.readInt()][];
        for (int k = 0; k < dims.length; k++) {
          int len = in.readInt();
          if (len != -1) {
            DimDim dimDim = incrementalIndex.getDimension(incrementalIndex.dimensions.get(k));
            String[] col = new String[len];
            for (int l = 0; l < col.length; l++) {
              col[l] = dimDim.get(dimDim.getValue(in.readInt()));
            }
            dims[k] = col;
          }
        }
        ret[i] = new TimeAndDims(timeStamp, dims);
      }
      return ret;
    }

    @Override
    public Comparator<TimeAndDims> getComparator()
    {
      return comparator;
    }
  }

  public static class TimeAndDimsComparator implements Comparator, Serializable
  {
    @Override
    public int compare(Object o1, Object o2)
    {
      return ((TimeAndDims) o1).compareTo((TimeAndDims) o2);
    }
  }

  private class OffheapDimDim implements DimDim
  {
    private final Map<String, Integer> falseIds;
    private final Map<Integer, String> falseIdsReverse;
    private final WeakHashMap<String, WeakReference<String>> cache =
        new WeakHashMap();
    private volatile String[] sortedVals = null;
    // size on MapDB is slow so maintain a count here
    private volatile int size = 0;

    public OffheapDimDim(String dimension)
    {
      falseIds = db.createHashMap(dimension)
                   .keySerializer(Serializer.STRING)
                   .valueSerializer(Serializer.INTEGER)
                   .make();
      falseIdsReverse = db.createHashMap(dimension + "_inverse")
                          .keySerializer(Serializer.INTEGER)
                          .valueSerializer(Serializer.STRING)
                          .make();
    }

    /**
     * Returns the interned String value to allow fast comparisons using `==` instead of `.equals()`
     *
     * @see io.druid.segment.incremental.IncrementalIndexStorageAdapter.EntryHolderValueMatcherFactory#makeValueMatcher(String, String)
     */
    public String get(String str)
    {
      final WeakReference<String> cached = cache.get(str);
      if (cached != null) {
        final String value = cached.get();
        if (value != null) {
          return value;
        }
      }
      cache.put(str, new WeakReference(str));
      return str;
    }

    public int getId(String value)
    {
      return falseIds.get(value);
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
      return size;
    }

    public synchronized int add(String value)
    {
      int id = size++;
      falseIds.put(value, id);
      falseIdsReverse.put(id, value);
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
      return s1.equals(s2);
    }
  }

}
