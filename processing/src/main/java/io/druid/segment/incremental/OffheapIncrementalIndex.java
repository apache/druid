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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.Store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

@Deprecated
/**
 * This is not yet ready for production use and requires more work.
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private static final long STORE_CHUNK_SIZE;

  static
  {
    // MapDB allocated memory in chunks. We need to know CHUNK_SIZE
    // in order to get a crude estimate of how much more direct memory
    // might be used when adding an additional row.
    try {
      Field field = Store.class.getDeclaredField("CHUNK_SIZE");
      field.setAccessible(true);
      STORE_CHUNK_SIZE = field.getLong(null);
    } catch(NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Unable to determine MapDB store chunk size", e);
    }
  }

  private final ResourceHolder<ByteBuffer> bufferHolder;

  private final DB db;
  private final DB factsDb;
  private final int[] aggPositionOffsets;
  private final int totalAggSize;
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;
  private final int maxTotalBufferSize;

  private String outOfRowsReason = null;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      StupidPool<ByteBuffer> bufferPool,
      boolean deserializeComplexMetrics,
      int maxTotalBufferSize
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);

    this.bufferHolder = bufferPool.take();
    Preconditions.checkArgument(
        maxTotalBufferSize > bufferHolder.get().limit(),
        "Maximum total buffer size must be greater than aggregation buffer size"
    );

    final AggregatorFactory[] metrics = incrementalIndexSchema.getMetrics();
    this.aggPositionOffsets = new int[metrics.length];

    int currAggSize = 0;
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggPositionOffsets[i] = currAggSize;
      currAggSize += agg.getMaxIntermediateSize();
    }
    this.totalAggSize = currAggSize;

    final DBMaker dbMaker = DBMaker.newMemoryDirectDB()
                                   .transactionDisable()
                                   .asyncWriteEnable()
                                   .cacheLRUEnable()
                                   .cacheSize(16384);

    this.factsDb = dbMaker.make();
    this.db = dbMaker.make();
    final TimeAndDimsSerializer timeAndDimsSerializer = new TimeAndDimsSerializer(this);
    this.facts = factsDb.createTreeMap("__facts" + UUID.randomUUID())
                        .keySerializer(timeAndDimsSerializer)
                        .comparator(timeAndDimsSerializer.getComparator())
                        .valueSerializer(Serializer.INTEGER)
                        .make();
    this.maxTotalBufferSize = maxTotalBufferSize;
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      StupidPool<ByteBuffer> bufferPool,
      boolean deserializeComplexMetrics,
      int maxTotalBufferSize
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        bufferPool,
        deserializeComplexMetrics,
        maxTotalBufferSize
    );
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension)
  {
    return new OffheapDimDim(dimension);
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics
  )
  {
    BufferAggregator[] aggs = new BufferAggregator[metrics.length];
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorizeBuffered(
          makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics)
      );
    }
    return aggs;
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    final BufferAggregator[] aggs = getAggs();
    Integer rowOffset;
    synchronized (this) {
      if (!facts.containsKey(key)) {
        if (!canAppendRow(false)) {
          throw new IndexSizeExceededException("%s", getOutOfRowsReason());
        }
      }
      rowOffset = totalAggSize * numEntries.get();
      final Integer prev = facts.putIfAbsent(key, rowOffset);
      if (prev != null) {
        rowOffset = prev;
      } else {
        numEntries.incrementAndGet();
        for (int i = 0; i < aggs.length; i++) {
          aggs[i].init(bufferHolder.get(), getMetricPosition(rowOffset, i));
        }
      }
    }
    rowContainer.set(row);
    for (int i = 0; i < aggs.length; i++) {
      synchronized (aggs[i]) {
        aggs[i].aggregate(bufferHolder.get(), getMetricPosition(rowOffset, i));
      }
    }
    rowContainer.set(null);
    return numEntries.get();
  }

  public boolean canAppendRow() {
    return canAppendRow(true);
  }

  private boolean canAppendRow(boolean includeFudgeFactor)
  {
    // there is a race condition when checking current MapDB
    // when canAppendRow() is called after adding a row it may return true, but on a subsequence call
    // to addToFacts that may not be the case anymore because MapDB size may have changed.
    // so we add this fudge factor, hoping that will be enough.

    final int aggBufferSize = bufferHolder.get().limit();
    if ((size() + 1) * totalAggSize > aggBufferSize) {
      outOfRowsReason = String.format("Maximum aggregation buffer limit reached [%d bytes].", aggBufferSize);
      return false;
    }
    // hopefully both MapDBs will grow by at most STORE_CHUNK_SIZE each when we add the next row.
    if (getCurrentSize() + totalAggSize + 2 * STORE_CHUNK_SIZE + (includeFudgeFactor ? STORE_CHUNK_SIZE : 0) > maxTotalBufferSize) {
      outOfRowsReason = String.format("Maximum time and dimension buffer limit reached [%d bytes].", maxTotalBufferSize - aggBufferSize);
      return false;
    }
    return true;
  }

  public String getOutOfRowsReason() {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get(bufferHolder.get(), getMetricPosition(rowOffset, aggPosition));
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return getAggs()[aggOffset].getFloat(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return getAggs()[aggOffset].getLong(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return getAggs()[aggOffset].get(bufferHolder.get(), getMetricPosition(rowOffset, aggOffset));
  }

  @Override
  public void close()
  {
    try {
      bufferHolder.close();
      Store.forDB(db).close();
      Store.forDB(factsDb).close();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private int getMetricPosition(int rowOffset, int metricIndex)
  {
    return rowOffset + aggPositionOffsets[metricIndex];
  }

  private DimDim getDimDim(int dimIndex)
  {
    return getDimValues().get(getDimensions().get(dimIndex));
  }

  // MapDB forces serializers to implement serializable, which sucks
  private static class TimeAndDimsSerializer extends BTreeKeySerializer<TimeAndDims> implements Serializable
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
            DimDim dimDim = incrementalIndex.getDimDim(index);
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
            DimDim dimDim = incrementalIndex.getDimDim(k);
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

  private static class TimeAndDimsComparator implements Comparator, Serializable
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
    private final WeakHashMap<String, WeakReference<String>> cache = new WeakHashMap();

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

  private long getCurrentSize()
  {
    return Store.forDB(db).getCurrSize() +
           Store.forDB(factsDb).getCurrSize()
           // Size of aggregators
           + size() * totalAggSize;
  }
}
