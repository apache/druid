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


import com.metamx.common.ISE;
import io.druid.collections.StupidPool;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class OffheapIncrementalIndex extends IncrementalIndex
{
  private volatile DB db;
  private volatile DB factsDb;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, bufferPool);
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      StupidPool<ByteBuffer> bufferPool,
      boolean deserializeComplexMetrics

  )
  {
    super(minTimestamp, gran, metrics, bufferPool, deserializeComplexMetrics);
  }

  @Override
  protected synchronized ConcurrentNavigableMap<TimeAndDims, Integer> createFactsTable()
  {
    if (factsDb == null) {
      final DBMaker dbMaker = DBMaker.newMemoryDirectDB()
                                     .transactionDisable()
                                     .asyncWriteEnable()
                                     .cacheSoftRefEnable();
      factsDb = dbMaker.make();
      db = dbMaker.make();
    }
    final TimeAndDimsSerializer timeAndDimsSerializer = new TimeAndDimsSerializer(this);
    return factsDb.createTreeMap("__facts" + UUID.randomUUID())
                  .keySerializer(timeAndDimsSerializer)
                  .comparator(timeAndDimsSerializer.getComparator())
                  .valueSerializer(Serializer.INTEGER)
                  .make();
  }

  @Override
  protected DimDim createDimDim(String dimension)
  {
    return new OffheapDimDim(dimension);
  }

  public static class TimeAndDimsSerializer extends BTreeKeySerializer<TimeAndDims> implements Serializable
  {
    private final TimeAndDimsComparator comparator;
    private final transient IncrementalIndex incrementalIndex;

    TimeAndDimsSerializer(IncrementalIndex incrementalIndex)
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
