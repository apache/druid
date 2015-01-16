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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.Store;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private static final long STORE_CHUNK_SIZE;
  private static final Object DB_CREATION_LOCKER = new Object();
  private static final DBMaker DB_MAKER = DBMaker
      .newMemoryDirectDB()
      .transactionDisable()
      .asyncWriteEnable()
      .cacheLRUEnable()
      .closeOnJvmShutdown()
      .deleteFilesAfterClose()
      .cacheSize(16384);
  private static final int BUFFER_LOCK_STRIPE_COUNT = 64;
  private static final int TYPICAL_BLOCK_SIZE = Long.SIZE / 8;

  private static DB db;
  private static DB factsDb;

  static {
    // MapDB allocated memory in chunks. We need to know CHUNK_SIZE
    // in order to get a crude estimate of how much more direct memory
    // might be used when adding an additional row.
    try {
      Field field = Store.class.getDeclaredField("CHUNK_SIZE");
      field.setAccessible(true);
      STORE_CHUNK_SIZE = field.getLong(null);
    }
    catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Unable to determine MapDB store chunk size", e);
    }
  }

  private final ResourceHolder<ByteBuffer> bufferHolder;
  private final ByteBuffer byteBuffer;

  private final int[] aggPositionOffsets;
  private final int totalAggSize;
  private final BTreeMap<TimeAndDims, Integer> facts;
  private final int maxTotalBufferSize;
  private final String factDbTableName = "__facts" + UUID.randomUUID();
  final Lock[] bufferStripeLock = new ReentrantLock[BUFFER_LOCK_STRIPE_COUNT];

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
    this.byteBuffer = bufferHolder.get();
    /** Currently synchronization happens on getApplyFunction, I'm leaving this here as a warning for future developers
     Preconditions.checkArgument(
     byteBuffer.isDirect(),
     "ByteBuffer from pool must be direct, or else buffer aggregations will fail"
     // Even direct byte buffers are not guaranteed to have atomic operations,
     // but tests indicate that they are atomic on x86 platforms on Oracle JVMs
     );
     */
    Preconditions.checkArgument(
        maxTotalBufferSize > byteBuffer.limit(),
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


    if (null == factsDb) {
      synchronized (DB_CREATION_LOCKER) {
        // check for race
        if (null == factsDb) {
          factsDb = DB_MAKER.make();
          db = DB_MAKER.make();
        }
      }
    }
    final TimeAndDimsSerializer timeAndDimsSerializer = new TimeAndDimsSerializer(this);
    this.facts = factsDb.createTreeMap(factDbTableName)
                        .keySerializer(timeAndDimsSerializer)
                        .comparator(timeAndDimsSerializer.getComparator())
                        .valueSerializer(Serializer.INTEGER)
                        .make();
    this.maxTotalBufferSize = maxTotalBufferSize;
    for (int i = 0; i < bufferStripeLock.length; ++i) {
      bufferStripeLock[i] = new ReentrantLock();
    }
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
  protected List<BufferAggregator> initAggs(
      AggregatorFactory[] metrics,
      ThreadLocal<InputRow> in,
      boolean deserializeComplexMetrics
  )
  {
    List<BufferAggregator> aggs = new ArrayList<BufferAggregator>(metrics.length);
    for (AggregatorFactory aggregatorFactory : metrics) {
      aggs.add(
          aggregatorFactory.factorizeBuffered(
              makeColumnSelectorFactory(
                  aggregatorFactory,
                  in,
                  deserializeComplexMetrics
              )
          )
      );
    }
    return aggs;
  }

  public boolean canAppendRow()
  {
    return canAppendRow(true);
  }

  private boolean canAppendRow(boolean includeFudgeFactor)
  {
    // there is a race condition when checking current MapDB
    // when canAppendRow() is called after adding a row it may return true, but on a subsequent call
    // to addToFacts that may not be the case anymore because MapDB size may have changed.
    // so we add this fudge factor, hoping that will be enough.

    final int aggBufferSize = byteBuffer.limit();
    if ((size() + 1) * totalAggSize > aggBufferSize) {
      outOfRowsReason = String.format("Maximum aggregation buffer limit reached [%d bytes].", aggBufferSize);
      return false;
    }
    // hopefully both MapDBs will grow by at most STORE_CHUNK_SIZE each when we add the next row.
    if (getCurrentSize() + totalAggSize + 2 * STORE_CHUNK_SIZE + (includeFudgeFactor ? STORE_CHUNK_SIZE : 0)
        > maxTotalBufferSize) {
      outOfRowsReason = String.format(
          "Maximum time and dimension buffer limit reached [%d bytes].",
          maxTotalBufferSize - aggBufferSize
      );
      return false;
    }
    return true;
  }

  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected List<BufferAggregator> getAggsForRow(int rowIndex)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowIndex, int aggPosition)
  {
    return agg.get(byteBuffer, getMetricPosition(rowIndex * totalAggSize, aggPosition));
  }

  @Override
  public float getMetricFloatValue(int rowIndex, int aggOffset)
  {
    return getAggs().get(aggOffset).getFloat(byteBuffer, getMetricPosition(rowIndex * totalAggSize, aggOffset));
  }

  @Override
  public long getMetricLongValue(int rowIndex, int aggOffset)
  {
    return getAggs().get(aggOffset).getLong(byteBuffer, getMetricPosition(rowIndex * totalAggSize, aggOffset));
  }

  @Override
  public Object getMetricObjectValue(int rowIndex, int aggOffset)
  {
    return getAggs().get(aggOffset).get(byteBuffer, getMetricPosition(rowIndex * totalAggSize, aggOffset));
  }

  private static Exception addSuppressed(Exception ex, Exception e)
  {
    if (null == ex) {
      return e;
    }
    ex.addSuppressed(e);
    return ex;
  }

  @Override
  public void close()
  {
    Exception ex = null;
    try {
      bufferHolder.close();
    }
    catch (Exception e) {
      ex = e;
    }
    try {
      super.close();
    }
    catch (Exception e) {
      ex = addSuppressed(ex, e);
    }
    try {
      facts.getEngine().clearCache(); // MapDB gets stupid about facts.clear() if the cache has stuff in it.
      factsDb.delete(factDbTableName);
    }
    catch (Exception e) {
      ex = addSuppressed(ex, e);
    }

    if (null != ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  protected Function<ColumnSelectorFactory, BufferAggregator> getFactorizeFunction(
      final AggregatorFactory agg
  )
  {
    return new Function<ColumnSelectorFactory, BufferAggregator>()
    {
      @Nullable
      @Override
      public BufferAggregator apply(ColumnSelectorFactory input)
      {
        return agg.factorizeBuffered(input);
      }
    };
  }

  private static int getLockStripe(final int position)
  {
    // Since most byte-wise positions will be divisible by some multiple of the underlying data, we make some general
    // assumptions to try and better spread make dense use of the lock striping
    return (position / TYPICAL_BLOCK_SIZE) % BUFFER_LOCK_STRIPE_COUNT;
  }

  @Override
  protected void applyAggregators(Integer rowIndex)
  {
    int metricIndex = 0;
    for (BufferAggregator agg : concurrentGet(rowIndex)) {
      final int position = getMetricPosition(rowIndex * totalAggSize, metricIndex);
      final Lock lock = bufferStripeLock[getLockStripe(position)];
      lock.lock();
      try {
        agg.aggregate(byteBuffer, getMetricPosition(rowIndex * totalAggSize, metricIndex));
      }
      finally {
        lock.unlock();
      }
      ++metricIndex;
    }
  }

  @Override
  protected List<BufferAggregator> concurrentGet(int rowIndex)
  {
    return getAggs();
  }

  @Override
  protected void concurrentSet(int rowIndex, List<BufferAggregator> value)
  {
    //NOOP
  }

  @Override
  protected void initializeAggs(List<BufferAggregator> aggs, Integer rowIndex)
  {
    for (int i = 0; i < aggs.size(); ++i) {
      aggs.get(i).init(byteBuffer, getMetricPosition(rowIndex * totalAggSize, i));
    }
  }

  private int getMetricPosition(int rowOffset, int metricIndex)
  {
    return rowOffset + aggPositionOffsets[metricIndex];
  }

  private DimDim getDimDim(String key)
  {
    return getDimValues().get(key);
  }

  private DimDim getDimDim(int dimIndex)
  {
    final List<String> dimNames = getDimensions();
    final int size = dimNames.size();
    if (dimIndex >= size) {
      throw new IndexOutOfBoundsException(String.format("Looking for index [%d] in size [%d]", dimIndex, size));
    }
    return getDimDim(dimNames.get(dimIndex));
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
        String[][] dimDims = timeAndDim.getDims();

        List<String> dimensions = incrementalIndex.getDimensions();

        if (dimDims.length > dimensions.size()) {
          throw new ISE("Bad dimension cache state");
        }
        int dimIndex = 0;
        for (String[] dims : timeAndDim.getDims()) {
          if (dims == null) {
            out.write(-1);
          } else {
            DimDim dimDim = incrementalIndex.getDimDim(dimensions.get(dimIndex));
            out.writeInt(dims.length);
            for (String value : dims) {
              final int id = dimDim.getId(value);
              if (OffheapDimDim.ERROR_CLOSED == id) {
                return; // We were closed while this was going on. Just skip the rest and return.
              }
              out.writeInt(id);
            }
          }
          ++dimIndex;
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
              col[l] = dimDim.intern(dimDim.getValue(in.readInt()));
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

  protected class OffheapDimDim implements DimDim
  {
    private static final int ERROR_CLOSED = -2;
    private final ConcurrentMap<String, Integer> falseIds;
    private final String falseIdTableName;
    private final ConcurrentMap<Integer, String> falseIdsReverse;
    private final String falseIdInverseTableName;
    // Unfortunately, MapMaker's weak keys are evaluated using == instead of .equals, otherwise we'd use them
    private final Map<String, WeakReference<String>> cache = new WeakHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final AtomicReference<String[]> sortedVals = new AtomicReference<>();
    // size on MapDB is slow so maintain a count here
    private final AtomicInteger size = new AtomicInteger(0);

    public OffheapDimDim(String dimension)
    {
      falseIdTableName = dimension + UUID.randomUUID();
      falseIds = db.createHashMap(falseIdTableName)
                   .keySerializer(Serializer.STRING)
                   .valueSerializer(Serializer.INTEGER)
                   .counterEnable()
                   .make();

      falseIdInverseTableName = dimension + "_inverse" + UUID.randomUUID();
      falseIdsReverse = db.createHashMap(falseIdInverseTableName)
                          .keySerializer(Serializer.INTEGER)
                          .valueSerializer(Serializer.STRING)
                          .make();
    }

    /**
     * Returns the interned String value to allow fast comparisons using `==` instead of `.equals()`
     *
     * @see io.druid.segment.incremental.IncrementalIndexStorageAdapter.EntryHolderValueMatcherFactory#makeValueMatcher(String, String)
     */
    @Override
    public String intern(String str)
    {
      WeakReference<String> cached = cache.get(str);
      if (cached == null || cached.get() == null) {
        synchronized (cache) {
          cached = cache.get(str);
          if (cached != null) {
            final String value = cached.get();
            if (value != null) {
              return value;
            }
          }
          cache.put(str, new WeakReference<String>(str));
          return str;
        }
      } else {
        return cached.get();
      }
    }

    @Override
    public int getId(String value)
    {
      if (isClosed.get()) {
        return ERROR_CLOSED; // special error code
      }
      return falseIds.get(value);
    }

    @Override
    public String getValue(int id)
    {
      if (isClosed.get()) {
        return null;
      }
      Preconditions.checkArgument(id >= 0, "id must be greater than or equal to 0");
      return falseIdsReverse.get(id);
    }

    @Override
    public boolean contains(String value)
    {
      return falseIds.containsKey(value);
    }

    @Override
    public int size()
    {
      return falseIds.size();
    }

    private final Integer raceId = -1;

    @Override
    public int add(String value)
    {
      Integer priorId = falseIds.putIfAbsent(value, raceId);
      if (null != priorId) {
        return priorId;
      }
      final Integer id = size.getAndIncrement();
      if (null != falseIdsReverse.put(id, value)) {
        throw new ISE("Bad reverse falseId table state");
      }
      if (falseIds.put(value, id) > 0) {
        throw new ISE("Bad falseId table state");
      }
      return id;
    }

    @Override
    public int getSortedId(String value)
    {
      sort();
      return Arrays.binarySearch(sortedVals.get(), value);
    }

    @Override
    public String getSortedValue(int index)
    {
      sort();
      return sortedVals.get()[index];
    }

    private final Lock sortLock = new ReentrantLock();

    @Override
    public void sort()
    {
      String[] sortedVals = this.sortedVals.get();
      if (sortedVals != null && sortedVals.length == size()) {
        return;
      }
      sortLock.lock();
      try {
        sortedVals = this.sortedVals.get();
        if (sortedVals != null && sortedVals.length == size()) {
          return;
        }
        if (sortedVals == null || sortedVals.length != size()) {
          ArrayList<String> newVals = new ArrayList<>(falseIds.keySet());
          Collections.sort(newVals);
          this.sortedVals.set(newVals.toArray(new String[0]));
        }
      }
      finally {
        sortLock.unlock();
      }
    }

    @Override
    public boolean compareCannonicalValues(String s1, String s2)
    {
      return s1.equals(s2);
    }

    @Override
    public void close() throws IOException
    {
      falseIds.clear();
      falseIdsReverse.clear();
      IOException ex = null;
      try {
        db.delete(falseIdTableName);
      }
      catch (Exception e) {
        ex = new IOException("Error closing OffHeap DimDim");
        ex.addSuppressed(e);
      }
      try {
        db.delete(falseIdInverseTableName);
      }
      catch (Exception e) {
        if (ex == null) {
          ex = new IOException("Error closing OffHeap DimDim");
        }
        ex.addSuppressed(e);
      }
      isClosed.set(true);
      if (null != ex) {
        throw ex;
      }
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
