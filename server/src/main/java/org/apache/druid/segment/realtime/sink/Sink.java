/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.realtime.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Sink implements Iterable<FireHydrant>, Overshadowable<Sink>
{
  private static final IncrementalIndexAddResult NOT_WRITABLE = new IncrementalIndexAddResult(-1, -1, "not writable");

  private static final IncrementalIndexAddResult ALREADY_SWAPPED =
      new IncrementalIndexAddResult(-1, -1, "write after index swapped");
  private static final Logger log = new Logger(Sink.class);

  private final Object hydrantLock = new Object();
  private final Interval interval;
  private final DataSchema schema;
  private final ShardSpec shardSpec;
  private final String version;
  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<>();

  private final LinkedHashSet<String> dimOrder = new LinkedHashSet<>();

  // columns excluding current index (the in-memory fire hydrant), includes __time column
  @GuardedBy("hydrantLock")
  private final LinkedHashSet<String> columnsExcludingCurrIndex = new LinkedHashSet<>();

  // column types for columns in {@code columnsExcludingCurrIndex}
  private final Map<String, ColumnType> columnTypeExcludingCurrIndex = new HashMap<>();

  private final AtomicInteger numRowsExcludingCurrIndex = new AtomicInteger();

  private volatile FireHydrant currHydrant;
  private volatile boolean writable = true;

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppendableIndexSpec appendableIndexSpec,
      int maxRowsInMemory,
      long maxBytesInMemory
  )
  {
    this(
        interval,
        schema,
        shardSpec,
        version,
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        Collections.emptyList()
    );
  }

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppendableIndexSpec appendableIndexSpec,
      int maxRowsInMemory,
      long maxBytesInMemory,
      List<FireHydrant> hydrants
  )
  {
    this.schema = schema;
    this.shardSpec = shardSpec;
    this.interval = interval;
    this.version = version;
    this.appendableIndexSpec = appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;

    int maxCount = -1;
    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() <= maxCount) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
      maxCount = hydrant.getCount();
      Segment segment = hydrant.acquireSegment();
      try {
        overwriteIndexDimensions(segment);
        QueryableIndex index = segment.as(QueryableIndex.class);
        numRowsExcludingCurrIndex.addAndGet(index.getNumRows());
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(segment);
      }
    }
    this.hydrants.addAll(hydrants);

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Interval getInterval()
  {
    return interval;
  }

  public FireHydrant getCurrHydrant()
  {
    return currHydrant;
  }

  public IncrementalIndexAddResult add(InputRow row)
  {
    if (currHydrant == null) {
      throw new IAE("No currHydrant but given row[%s]", row);
    }

    synchronized (hydrantLock) {
      if (!writable) {
        return NOT_WRITABLE;
      }

      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return ALREADY_SWAPPED; // the hydrant was swapped without being replaced
      }

      return index.add(row);
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
      return writable && currHydrant.getIndex() != null && currHydrant.getIndex().numRows() != 0;
    }
  }

  public boolean finished()
  {
    return !writable;
  }

  /**
   * Marks sink as 'finished', preventing further writes.
   *
   * @return 'true' if sink was sucessfully finished, 'false' if sink was already finished
   */
  public boolean finishWriting()
  {
    synchronized (hydrantLock) {
      if (!writable) {
        return false;
      }
      writable = false;
    }
    return true;
  }

  public DataSegment getSegment()
  {
    return new DataSegment(
        schema.getDataSource(),
        interval,
        version,
        ImmutableMap.of(),
        Collections.emptyList(),
        Lists.transform(Arrays.asList(schema.getAggregators()), AggregatorFactory::getName),
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

      return index.numRows();
    }
  }

  public long getBytesInMemory()
  {
    synchronized (hydrantLock) {
      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return 0;
      }

      return index.getBytesInMemory().get();
    }
  }

  /**
   * Acquire references to all {@link FireHydrant} that represent this sink. Returns null if they cannot all be
   * acquired, possibly because they were closed (swapped to null) concurrently with this method being called.
   *
   * @param segmentMapFn           from {@link org.apache.druid.query.DataSource#createSegmentMapFunction(Query)}
   * @param skipIncrementalSegment whether in-memory {@link IncrementalIndex} segments should be skipped
   */
  @Nullable
  public List<SinkSegmentReference> acquireSegmentReferences(
      final SegmentMapFunction segmentMapFn,
      final boolean skipIncrementalSegment
  )
  {
    return acquireSegmentReferences(hydrants, segmentMapFn, skipIncrementalSegment);
  }

  private FireHydrant makeNewCurrIndex(long minTimestamp, DataSchema schema)
  {
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(minTimestamp)
        .withTimestampSpec(schema.getTimestampSpec())
        .withQueryGranularity(schema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(schema.getDimensionsSpec())
        .withMetrics(schema.getAggregators())
        .withRollup(schema.getGranularitySpec().isRollup())
        .withProjections(schema.getProjections())
        .build();

    // Build the incremental-index according to the spec that was chosen by the user
    final IncrementalIndex newIndex = appendableIndexSpec
        .builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(maxRowsInMemory)
        .setMaxBytesInMemory(maxBytesInMemory)
        .build();

    final FireHydrant old;
    synchronized (hydrantLock) {
      if (writable) {
        old = currHydrant;
        int newCount = 0;
        int numHydrants = hydrants.size();
        if (numHydrants > 0) {
          FireHydrant lastHydrant = hydrants.get(numHydrants - 1);
          Map<String, ColumnFormat> oldFormat = null;
          newCount = lastHydrant.getCount() + 1;

          boolean variableDimensions = !indexSchema.getDimensionsSpec().hasFixedDimensions();

          if (lastHydrant.hasSwapped()) {
            oldFormat = new HashMap<>();
            final Segment segment = lastHydrant.acquireSegment();
            try {
              overwriteIndexDimensions(segment);
              if (variableDimensions) {
                final QueryableIndex oldIndex = Preconditions.checkNotNull(segment.as(QueryableIndex.class));
                for (String dim : oldIndex.getAvailableDimensions()) {
                  dimOrder.add(dim);
                  oldFormat.put(dim, oldIndex.getColumnHolder(dim).getColumnFormat());
                }
              }
            }
            finally {
              CloseableUtils.closeAndWrapExceptions(segment);
            }
          } else {
            final Segment segment = lastHydrant.acquireSegment();
            try {
              overwriteIndexDimensions(segment);
              if (variableDimensions) {
                IncrementalIndex oldIndex = lastHydrant.getIndex();
                dimOrder.addAll(oldIndex.getDimensionOrder());
                oldFormat = oldIndex.getColumnFormats();
              }
            }
            finally {
              CloseableUtils.closeAndWrapExceptions(segment);
            }
          }
          if (variableDimensions) {
            newIndex.loadDimensionIterable(dimOrder, oldFormat);
          }
        }
        currHydrant = new FireHydrant(newIndex, newCount, getSegment().getId());
        if (old != null) {
          numRowsExcludingCurrIndex.addAndGet(old.getIndex().numRows());
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

  /**
   * Merge the column from the index with the existing columns.
   */
  @GuardedBy("hydrantLock")
  private void overwriteIndexDimensions(Segment segment)
  {
    RowSignature rowSignature = Objects.requireNonNull(segment.as(CursorFactory.class)).getRowSignature();
    for (String dim : rowSignature.getColumnNames()) {
      columnsExcludingCurrIndex.add(dim);
      rowSignature.getColumnType(dim).ifPresent(type -> columnTypeExcludingCurrIndex.put(dim, type));
    }
  }

  /**
   * Get column information from all the {@link FireHydrant}'s.
   */
  public RowSignature getSignature()
  {
    synchronized (hydrantLock) {
      RowSignature.Builder builder = RowSignature.builder();

      // Add columns from columnsExcludingCurrIndex.
      for (String dim : columnsExcludingCurrIndex) {
        builder.add(dim, columnTypeExcludingCurrIndex.get(dim));
      }

      // Add columns from the currHydrant that do not yet exist in columnsExcludingCurrIndex.
      RowSignature currSignature = currHydrant.getHydrantSegment()
                                              .getBaseSegment()
                                              .as(CursorFactory.class)
                                              .getRowSignature();

      for (String dim : currSignature.getColumnNames()) {
        if (!columnsExcludingCurrIndex.contains(dim)) {
          builder.add(dim, currSignature.getColumnType(dim).orElse(null));
        }
      }

      RowSignature rowSignature = builder.build();

      log.debug("Sink signature is [%s].", rowSignature);

      return rowSignature;
    }
  }

  @Override
  public Iterator<FireHydrant> iterator()
  {
    return Iterators.filter(
        hydrants.iterator(),
        new Predicate<>()
        {
          @Override
          public boolean apply(FireHydrant input)
          {
            final IncrementalIndex index = input.getIndex();
            return index == null || index.numRows() != 0;
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

  @Override
  public boolean overshadows(Sink other)
  {
    // Sink is currently used in timeline only for querying stream data.
    // In this case, sinks never overshadow each other.
    return false;
  }

  @Override
  public int getStartRootPartitionId()
  {
    return shardSpec.getStartRootPartitionId();
  }

  @Override
  public int getEndRootPartitionId()
  {
    return shardSpec.getEndRootPartitionId();
  }

  @Override
  public String getVersion()
  {
    return version;
  }

  @Override
  public short getMinorVersion()
  {
    return shardSpec.getMinorVersion();
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return shardSpec.getAtomicUpdateGroupSize();
  }

  /**
   * Helper for {@link #acquireSegmentReferences(SegmentMapFunction, boolean)}. Separate method to simplify testing (we test this
   * method instead of testing {@link #acquireSegmentReferences(SegmentMapFunction, boolean)} directly).
   */
  @VisibleForTesting
  static List<SinkSegmentReference> acquireSegmentReferences(
      final List<FireHydrant> hydrants,
      final SegmentMapFunction segmentMapFn,
      final boolean skipIncrementalSegment
  )
  {
    final List<SinkSegmentReference> retVal = new ArrayList<>(hydrants.size());

    try {
      for (final FireHydrant hydrant : hydrants) {
        // Hydrant might swap at any point, but if it's swapped at the start
        // then we know it's *definitely* swapped.
        final boolean hydrantDefinitelySwapped = hydrant.hasSwapped();

        if (skipIncrementalSegment && !hydrantDefinitelySwapped) {
          continue;
        }

        final Optional<Segment> maybeHolder = hydrant.getSegmentForQuery(segmentMapFn);
        if (maybeHolder.isPresent()) {
          final Segment holder = maybeHolder.get();
          retVal.add(new SinkSegmentReference(hydrant.getCount(), holder, hydrantDefinitelySwapped));
        } else {
          // Cannot acquire this hydrant. Release all others previously acquired and return null.
          for (final SinkSegmentReference reference : retVal) {
            reference.close();
          }

          return null;
        }
      }

      return retVal;
    }
    catch (Throwable e) {
      // Release all references previously acquired and throw the error.
      for (final SinkSegmentReference reference : retVal) {
        CloseableUtils.closeAndSuppressExceptions(reference, e::addSuppressed);
      }

      throw e;
    }
  }
}
