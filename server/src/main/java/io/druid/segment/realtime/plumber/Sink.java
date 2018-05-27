/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAddResult;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.FireHydrant;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Sink implements Iterable<FireHydrant>
{
  private static final IncrementalIndexAddResult ALREADY_SWAPPED = new IncrementalIndexAddResult(-1, -1, null, "write after index swapped");

  private final Object hydrantLock = new Object();
  private final Interval interval;
  private final DataSchema schema;
  private final ShardSpec shardSpec;
  private final String version;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean reportParseExceptions;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<FireHydrant>();
  private final LinkedHashSet<String> dimOrder = Sets.newLinkedHashSet();
  private final AtomicInteger numRowsExcludingCurrIndex = new AtomicInteger();
  private volatile FireHydrant currHydrant;
  private volatile boolean writable = true;
  private final String dedupColumn;
  private final Set<Long> dedupSet = new HashSet<>();

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      int maxRowsInMemory,
      long maxBytesInMemory,
      boolean reportParseExceptions,
      String dedupColumn
  )
  {
    this.schema = schema;
    this.shardSpec = shardSpec;
    this.interval = interval;
    this.version = version;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.reportParseExceptions = reportParseExceptions;
    this.dedupColumn = dedupColumn;

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      int maxRowsInMemory,
      long maxBytesInMemory,
      boolean reportParseExceptions,
      String dedupColumn,
      List<FireHydrant> hydrants
  )
  {
    this.schema = schema;
    this.shardSpec = shardSpec;
    this.interval = interval;
    this.version = version;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.reportParseExceptions = reportParseExceptions;
    this.dedupColumn = dedupColumn;

    int maxCount = -1;
    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() <= maxCount) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
      maxCount = hydrant.getCount();
      ReferenceCountingSegment segment = hydrant.getIncrementedSegment();
      try {
        numRowsExcludingCurrIndex.addAndGet(segment.asQueryableIndex().getNumRows());
      }
      finally {
        segment.decrement();
      }
    }
    this.hydrants.addAll(hydrants);

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public void clearDedupCache()
  {
    dedupSet.clear();
  }

  public String getVersion()
  {
    return version;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public FireHydrant getCurrHydrant()
  {
    return currHydrant;
  }

  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    if (currHydrant == null) {
      throw new IAE("No currHydrant but given row[%s]", row);
    }

    synchronized (hydrantLock) {
      if (!writable) {
        return Plumber.NOT_WRITABLE;
      }

      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return ALREADY_SWAPPED; // the hydrant was swapped without being replaced
      }

      if (checkInDedupSet(row)) {
        return Plumber.DUPLICATE;
      }

      return index.add(row, skipMaxRowsInMemoryCheck);
    }
  }

  public boolean canAppendRow()
  {
    synchronized (hydrantLock) {
      return writable && currHydrant != null && currHydrant.getIndex().canAppendRow();
    }
  }

  public boolean isEmpty()
  {
    synchronized (hydrantLock) {
      return hydrants.size() == 1 && currHydrant.getIndex().isEmpty();
    }
  }

  public boolean isWritable()
  {
    return writable;
  }

  /**
   * If currHydrant is A, creates a new index B, sets currHydrant to B and returns A.
   *
   * @return the current index after swapping in a new one
   */
  public FireHydrant swap()
  {
    return makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public boolean swappable()
  {
    synchronized (hydrantLock) {
      return writable && currHydrant.getIndex() != null && currHydrant.getIndex().size() != 0;
    }
  }

  public boolean finished()
  {
    return !writable;
  }

  public void finishWriting()
  {
    synchronized (hydrantLock) {
      writable = false;
      clearDedupCache();
    }
  }

  public DataSegment getSegment()
  {
    return new DataSegment(
        schema.getDataSource(),
        interval,
        version,
        ImmutableMap.<String, Object>of(),
        Lists.<String>newArrayList(),
        Lists.transform(
            Arrays.asList(schema.getAggregators()), new Function<AggregatorFactory, String>()
            {
              @Override
              public String apply(@Nullable AggregatorFactory input)
              {
                return input.getName();
              }
            }
        ),
        shardSpec,
        null,
        0
    );
  }

  public int getNumRows()
  {
    synchronized (hydrantLock) {
      return numRowsExcludingCurrIndex.get() + getNumRowsInMemory();
    }
  }

  public int getNumRowsInMemory()
  {
    synchronized (hydrantLock) {
      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return 0;
      }

      return currHydrant.getIndex().size();
    }
  }

  public long getBytesInMemory()
  {
    synchronized (hydrantLock) {
      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return 0;
      }

      return currHydrant.getIndex().getBytesInMemory();
    }
  }

  private boolean checkInDedupSet(InputRow row)
  {
    if (dedupColumn != null) {
      Object value = row.getRaw(dedupColumn);
      if (value != null) {
        if (value instanceof List) {
          throw new IAE("Dedup on multi-value field not support");
        }
        Long pk;
        if (value instanceof Long || value instanceof Integer) {
          pk = ((Number) value).longValue();
        } else {
          // use long type hashcode to reduce heap cost.
          // maybe hash collision, but it's more important to avoid OOM
          pk = pkHash(String.valueOf(value));
        }
        if (dedupSet.contains(pk)) {
          return true;
        }
        dedupSet.add(pk);
      }
    }
    return false;
  }

  private long pkHash(String s)
  {
    long seed = 131; // 31 131 1313 13131 131313 etc..  BKDRHash
    long hash = 0;
    for (int i = 0; i < s.length(); i++) {
      hash = (hash * seed) + s.charAt(i);
    }
    return hash;
  }

  private FireHydrant makeNewCurrIndex(long minTimestamp, DataSchema schema)
  {
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(minTimestamp)
        .withTimestampSpec(schema.getParser())
        .withQueryGranularity(schema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(schema.getParser())
        .withMetrics(schema.getAggregators())
        .withRollup(schema.getGranularitySpec().isRollup())
        .build();
    final IncrementalIndex newIndex = new IncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setReportParseExceptions(reportParseExceptions)
        .setMaxRowCount(maxRowsInMemory)
        .setMaxBytesInMemory(maxBytesInMemory)
        .buildOnheap();

    final FireHydrant old;
    synchronized (hydrantLock) {
      if (writable) {
        old = currHydrant;
        int newCount = 0;
        int numHydrants = hydrants.size();
        if (numHydrants > 0) {
          FireHydrant lastHydrant = hydrants.get(numHydrants - 1);
          newCount = lastHydrant.getCount() + 1;
          if (!indexSchema.getDimensionsSpec().hasCustomDimensions()) {
            Map<String, ColumnCapabilitiesImpl> oldCapabilities;
            if (lastHydrant.hasSwapped()) {
              oldCapabilities = Maps.newHashMap();
              ReferenceCountingSegment segment = lastHydrant.getIncrementedSegment();
              try {
                QueryableIndex oldIndex = segment.asQueryableIndex();
                for (String dim : oldIndex.getAvailableDimensions()) {
                  dimOrder.add(dim);
                  oldCapabilities.put(dim, (ColumnCapabilitiesImpl) oldIndex.getColumn(dim).getCapabilities());
                }
              }
              finally {
                segment.decrement();
              }
            } else {
              IncrementalIndex oldIndex = lastHydrant.getIndex();
              dimOrder.addAll(oldIndex.getDimensionOrder());
              oldCapabilities = oldIndex.getColumnCapabilities();
            }
            newIndex.loadDimensionIterable(dimOrder, oldCapabilities);
          }
        }
        currHydrant = new FireHydrant(newIndex, newCount, getSegment().getIdentifier());
        if (old != null) {
          numRowsExcludingCurrIndex.addAndGet(old.getIndex().size());
        }
        hydrants.add(currHydrant);
      } else {
        // Oops, someone called finishWriting while we were making this new index.
        newIndex.close();
        throw new ISE("finishWriting() called during swap");
      }
    }

    return old;
  }

  @Override
  public Iterator<FireHydrant> iterator()
  {
    return Iterators.filter(
        hydrants.iterator(),
        new Predicate<FireHydrant>()
        {
          @Override
          public boolean apply(FireHydrant input)
          {
            final IncrementalIndex index = input.getIndex();
            return index == null || index.size() != 0;
          }
        }
    );
  }

  @Override
  public String toString()
  {
    return "Sink{" +
           "interval=" + interval +
           ", schema=" + schema +
           '}';
  }
}
