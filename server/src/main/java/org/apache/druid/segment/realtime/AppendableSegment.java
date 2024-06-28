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

package org.apache.druid.segment.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnFormat;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class AppendableSegment implements Iterable<PartialSegment>, Overshadowable<AppendableSegment>
{
  private static final IncrementalIndexAddResult NOT_WRITABLE = new IncrementalIndexAddResult(-1, -1, "not writable");

  private static final IncrementalIndexAddResult ALREADY_SWAPPED =
      new IncrementalIndexAddResult(-1, -1, "write after index swapped");
  private static final Logger log = new Logger(AppendableSegment.class);

  private final Object partialSegmentLock = new Object();
  private final Interval interval;
  private final DataSchema schema;
  private final ShardSpec shardSpec;
  private final String version;
  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean useMaxMemoryEstimates;
  private final CopyOnWriteArrayList<PartialSegment> partialSegments = new CopyOnWriteArrayList<>();

  private final LinkedHashSet<String> dimOrder = new LinkedHashSet<>();

  // columns excluding current index (the in-memory PartialSegment), includes __time column
  private final LinkedHashSet<String> columnsExcludingCurrIndex = new LinkedHashSet<>();

  // column types for columns in {@code columnsExcludingCurrIndex}
  private final Map<String, ColumnType> columnTypeExcludingCurrIndex = new HashMap<>();

  private final AtomicInteger numRowsExcludingCurrIndex = new AtomicInteger();

  private volatile PartialSegment currentPartialSegment;
  private volatile boolean writable = true;

  public AppendableSegment(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppendableIndexSpec appendableIndexSpec,
      int maxRowsInMemory,
      long maxBytesInMemory,
      boolean useMaxMemoryEstimates
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
        useMaxMemoryEstimates,
        Collections.emptyList()
    );
  }

  public AppendableSegment(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppendableIndexSpec appendableIndexSpec,
      int maxRowsInMemory,
      long maxBytesInMemory,
      boolean useMaxMemoryEstimates,
      List<PartialSegment> partialSegments
  )
  {
    this.schema = schema;
    this.shardSpec = shardSpec;
    this.interval = interval;
    this.version = version;
    this.appendableIndexSpec = appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxBytesInMemory = maxBytesInMemory;
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;

    int maxCount = -1;
    for (int i = 0; i < partialSegments.size(); ++i) {
      final PartialSegment partialSegment = partialSegments.get(i);
      if (partialSegment.getCount() <= maxCount) {
        throw new ISE("partialSegment[%s] not the right count[%s]", partialSegment, i);
      }
      maxCount = partialSegment.getCount();
      ReferenceCountingSegment segment = partialSegment.getIncrementedSegment();
      try {
        QueryableIndex index = segment.asQueryableIndex();
        overwriteIndexDimensions(new QueryableIndexStorageAdapter(index));
        numRowsExcludingCurrIndex.addAndGet(index.getNumRows());
      }
      finally {
        segment.decrement();
      }
    }
    this.partialSegments.addAll(partialSegments);

    makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public Interval getInterval()
  {
    return interval;
  }

  public PartialSegment getCurrentPartialSegment()
  {
    return currentPartialSegment;
  }

  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    if (currentPartialSegment == null) {
      throw new IAE("No currentPartialSegment but given row[%s]", row);
    }

    synchronized (partialSegmentLock) {
      if (!writable) {
        return NOT_WRITABLE;
      }

      IncrementalIndex index = currentPartialSegment.getIndex();
      if (index == null) {
        return ALREADY_SWAPPED; // the partialSegment was swapped without being replaced
      }

      return index.add(row, skipMaxRowsInMemoryCheck);
    }
  }

  public boolean canAppendRow()
  {
    synchronized (partialSegmentLock) {
      return writable && currentPartialSegment != null && currentPartialSegment.getIndex().canAppendRow();
    }
  }

  public boolean isEmpty()
  {
    synchronized (partialSegmentLock) {
      return partialSegments.size() == 1 && currentPartialSegment.getIndex().isEmpty();
    }
  }

  public boolean isWritable()
  {
    return writable;
  }

  /**
   * If currPartialSegment is A, creates a new index B, sets currPartialSegment to B and returns A.
   *
   * @return the current index after swapping in a new one
   */
  public PartialSegment swap()
  {
    return makeNewCurrIndex(interval.getStartMillis(), schema);
  }

  public boolean swappable()
  {
    synchronized (partialSegmentLock) {
      return writable && currentPartialSegment.getIndex() != null && currentPartialSegment.getIndex().size() != 0;
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
    synchronized (partialSegmentLock) {
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
    synchronized (partialSegmentLock) {
      return numRowsExcludingCurrIndex.get() + getNumRowsInMemory();
    }
  }

  public int getNumRowsInMemory()
  {
    synchronized (partialSegmentLock) {
      IncrementalIndex index = currentPartialSegment.getIndex();
      if (index == null) {
        return 0;
      }

      return currentPartialSegment.getIndex().size();
    }
  }

  public long getBytesInMemory()
  {
    synchronized (partialSegmentLock) {
      IncrementalIndex index = currentPartialSegment.getIndex();
      if (index == null) {
        return 0;
      }

      return currentPartialSegment.getIndex().getBytesInMemory().get();
    }
  }

  /**
   * Acquire references to all {@link PartialSegment} that represent this sink. Returns null if they cannot all be
   * acquired, possibly because they were closed (swapped to null) concurrently with this method being called.
   *
   * @param segmentMapFn           from {@link org.apache.druid.query.DataSource#createSegmentMapFunction(Query, AtomicLong)}
   * @param skipIncrementalSegment whether in-memory {@link IncrementalIndex} segments should be skipped
   */
  @Nullable
  public List<PartialSegmentReference> acquireSegmentReferences(
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final boolean skipIncrementalSegment
  )
  {
    return acquireSegmentReferences(partialSegments, segmentMapFn, skipIncrementalSegment);
  }

  private PartialSegment makeNewCurrIndex(long minTimestamp, DataSchema schema)
  {
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(minTimestamp)
        .withTimestampSpec(schema.getTimestampSpec())
        .withQueryGranularity(schema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(schema.getDimensionsSpec())
        .withMetrics(schema.getAggregators())
        .withRollup(schema.getGranularitySpec().isRollup())
        .build();

    // Build the incremental-index according to the spec that was chosen by the user
    final IncrementalIndex newIndex = appendableIndexSpec
        .builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(maxRowsInMemory)
        .setMaxBytesInMemory(maxBytesInMemory)
        .setUseMaxMemoryEstimates(useMaxMemoryEstimates)
        .build();

    final PartialSegment old;
    synchronized (partialSegmentLock) {
      if (writable) {
        old = currentPartialSegment;
        int newCount = 0;
        int numPartialSEgments = partialSegments.size();
        if (numPartialSEgments > 0) {
          PartialSegment lastParialSegment = partialSegments.get(numPartialSEgments - 1);
          Map<String, ColumnFormat> oldFormat = null;
          newCount = lastParialSegment.getCount() + 1;

          boolean customDimensions = !indexSchema.getDimensionsSpec().hasCustomDimensions();

          if (lastParialSegment.hasSwapped()) {
            oldFormat = new HashMap<>();
            ReferenceCountingSegment segment = lastParialSegment.getIncrementedSegment();
            try {
              QueryableIndex oldIndex = segment.asQueryableIndex();
              overwriteIndexDimensions(new QueryableIndexStorageAdapter(oldIndex));
              if (customDimensions) {
                for (String dim : oldIndex.getAvailableDimensions()) {
                  dimOrder.add(dim);
                  oldFormat.put(dim, oldIndex.getColumnHolder(dim).getColumnFormat());
                }
              }
            }
            finally {
              segment.decrement();
            }
          } else {
            IncrementalIndex oldIndex = lastParialSegment.getIndex();
            overwriteIndexDimensions(new IncrementalIndexStorageAdapter(oldIndex));
            if (customDimensions) {
              dimOrder.addAll(oldIndex.getDimensionOrder());
              oldFormat = oldIndex.getColumnFormats();
            }
          }
          if (customDimensions) {
            newIndex.loadDimensionIterable(dimOrder, oldFormat);
          }
        }
        currentPartialSegment = new PartialSegment(newIndex, newCount, getSegment().getId());
        if (old != null) {
          numRowsExcludingCurrIndex.addAndGet(old.getIndex().size());
        }
        partialSegments.add(currentPartialSegment);
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
  private void overwriteIndexDimensions(StorageAdapter storageAdapter)
  {
    RowSignature rowSignature = storageAdapter.getRowSignature();
    for (String dim : rowSignature.getColumnNames()) {
      columnsExcludingCurrIndex.add(dim);
      rowSignature.getColumnType(dim).ifPresent(type -> columnTypeExcludingCurrIndex.put(dim, type));
    }
  }

  /**
   * Get column information from all the {@link PartialSegment}'s.
   */
  public RowSignature getSignature()
  {
    synchronized (partialSegmentLock) {
      RowSignature.Builder builder = RowSignature.builder();

      builder.addTimeColumn();

      for (String dim : columnsExcludingCurrIndex) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(dim)) {
          builder.add(dim, columnTypeExcludingCurrIndex.get(dim));
        }
      }

      IncrementalIndexStorageAdapter incrementalIndexStorageAdapter = new IncrementalIndexStorageAdapter(
          currentPartialSegment.getIndex());
      RowSignature incrementalIndexSignature = incrementalIndexStorageAdapter.getRowSignature();

      for (String dim : incrementalIndexSignature.getColumnNames()) {
        if (!columnsExcludingCurrIndex.contains(dim) && !ColumnHolder.TIME_COLUMN_NAME.equals(dim)) {
          builder.add(dim, incrementalIndexSignature.getColumnType(dim).orElse(null));
        }
      }

      RowSignature rowSignature = builder.build();

      log.debug("Sink signature is [%s].", rowSignature);

      return rowSignature;
    }
  }

  @Override
  public Iterator<PartialSegment> iterator()
  {
    return Iterators.filter(
        partialSegments.iterator(),
        new Predicate<PartialSegment>()
        {
          @Override
          public boolean apply(PartialSegment input)
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

  @Override
  public boolean overshadows(AppendableSegment other)
  {
    // Sink is currently used in timeline only for querying stream data.
    // In this case, AppendableSegment never overshadow each other.
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
   * Helper for {@link #acquireSegmentReferences(Function, boolean)}. Separate method to simplify testing (we test this
   * method instead of testing {@link #acquireSegmentReferences(Function, boolean)} directly).
   */
  @Nullable
  @VisibleForTesting
  static List<PartialSegmentReference> acquireSegmentReferences(
      final List<PartialSegment> partialSegments,
      final Function<SegmentReference, SegmentReference> segmentMapFn,
      final boolean skipIncrementalSegment
  )
  {
    final List<PartialSegmentReference> retVal = new ArrayList<>(partialSegments.size());

    try {
      for (final PartialSegment partialSegment : partialSegments) {
        // PartialSegment might swap at any point, but if it's swapped at the start
        // then we know it's *definitely* swapped.
        final boolean partialSegmentDefinitelySwapped = partialSegment.hasSwapped();

        if (skipIncrementalSegment && !partialSegmentDefinitelySwapped) {
          continue;
        }

        final Optional<Pair<SegmentReference, Closeable>> maybeHolder = partialSegment.getSegmentForQuery(segmentMapFn);
        if (maybeHolder.isPresent()) {
          final Pair<SegmentReference, Closeable> holder = maybeHolder.get();
          retVal.add(new PartialSegmentReference(partialSegment.getCount(), holder.lhs, partialSegmentDefinitelySwapped, holder.rhs));
        } else {
          // Cannot acquire this partialSegment. Release all others previously acquired and return null.
          for (final PartialSegmentReference reference : retVal) {
            reference.close();
          }

          return null;
        }
      }

      return retVal;
    }
    catch (Throwable e) {
      // Release all references previously acquired and throw the error.
      for (final PartialSegmentReference reference : retVal) {
        CloseableUtils.closeAndSuppressExceptions(reference, e::addSuppressed);
      }

      throw e;
    }
  }
}
