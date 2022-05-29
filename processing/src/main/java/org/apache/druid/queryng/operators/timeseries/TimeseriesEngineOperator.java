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

package org.apache.druid.queryng.operators.timeseries;

import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.timeseries.TimeseriesResultBuilder;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Iterators.CountingResultIterator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.queryng.operators.SequenceIterator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * "Engine" operator for the time series query. Each query performs an aggregation over a
 * time series for a single segment. The operator supports both vectorized and non-vectorized
 * operations. The two implementations are in distinct result iterators: allowing the engine
 * operator to act as a generic holder for both.
 * <p>
 * An engine pushes down to the storage layer both filter and aggregation operations. Filtering
 * is pushed for the query as a whole; aggregations are pushed per group (that is, per time
 * bucket.)
 */
public class TimeseriesEngineOperator implements Operator<Result<TimeseriesResultValue>>
{
  public static class CursorDefinition
  {
    private final StorageAdapter adapter;
    private final Interval queryInterval;
    @Nullable private final Filter filter;
    @Nullable private final VirtualColumns virtualColumns;
    private final boolean descending;
    private final Granularity granularity;
    @Nullable private final QueryMetrics<?> queryMetrics;
    private final int vectorSize;

    public CursorDefinition(
        final StorageAdapter adapter,
        final Interval queryInterval,
        @Nullable final Filter filter,
        @Nullable final VirtualColumns virtualColumns,
        final boolean descending,
        final Granularity granularity,
        @Nullable final QueryMetrics<?> queryMetrics,
        final int vectorSize
    )
    {
      this.adapter = adapter;
      this.queryInterval = queryInterval;
      this.filter = filter;
      this.virtualColumns = virtualColumns;
      this.descending = descending;
      this.granularity = granularity;
      this.queryMetrics = queryMetrics;
      this.vectorSize = vectorSize;
    }

    public SequenceIterator<Cursor> cursors()
    {
      return SequenceIterator.of(
          adapter.makeCursors(
              filter,
              queryInterval,
              virtualColumns,
              granularity,
              descending,
              queryMetrics
          )
      );
    }

    public VectorCursor vectorCursor()
    {
      return adapter.makeVectorCursor(
          filter,
          queryInterval,
          virtualColumns,
          descending,
          vectorSize,
          queryMetrics
      );
    }
  }

  /**
   * Class that delivers results on top of the native engine.
   */
  public abstract static class BaseResultIterator extends CountingResultIterator<Result<TimeseriesResultValue>>
  {
    protected final List<AggregatorFactory> aggregatorSpecs;
    protected final boolean skipEmptyBuckets;
    protected final String[] aggregatorNames;

    public BaseResultIterator(
        final List<AggregatorFactory> aggregatorSpecs,
        final boolean skipEmptyBuckets
    )
    {
      this.aggregatorSpecs = aggregatorSpecs;
      this.skipEmptyBuckets = skipEmptyBuckets;
      this.aggregatorNames = new String[aggregatorSpecs.size()];
      for (int i = 0; i < aggregatorSpecs.size(); i++) {
        aggregatorNames[i] = aggregatorSpecs.get(i).getName();
      }
    }

    protected abstract void close(OperatorProfile profile);
  }

  /**
   * Non-vectorized. The storage adapter provides one or more cursors: one for each
   * contiguous span of times.
   */
  public static class NonVectorizedResultIterator extends BaseResultIterator
  {
    private final SequenceIterator<Cursor> cursorIter;
    private int cursorCount;

    public NonVectorizedResultIterator(
        final SequenceIterator<Cursor> cursorIter,
        final List<AggregatorFactory> aggregatorSpecs,
        final boolean skipEmptyBuckets
    )
    {
      super(
          aggregatorSpecs,
          skipEmptyBuckets
      );
      this.cursorIter = cursorIter;
    }

    @Override
    public Result<TimeseriesResultValue> next() throws ResultIterator.EofException
    {
      while (true) {
        if (!cursorIter.hasNext()) {
          throw Operators.eof();
        }
        Cursor cursor = cursorIter.next();
        cursorCount++;
        if (skipEmptyBuckets && cursor.isDone()) {
          continue;
        }

        ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
        Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
        for (int i = 0; i < aggregatorSpecs.size(); i++) {
          aggregators[i] = aggregatorSpecs.get(i).factorize(columnSelectorFactory);
        }

        try {
          while (!cursor.isDone()) {
            for (Aggregator aggregator : aggregators) {
              aggregator.aggregate();
            }
            rowCount++;
            cursor.advance();
          }

          TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime());
          for (int i = 0; i < aggregators.length; i++) {
            bob.addMetric(aggregatorNames[i], aggregators[i].get());
          }
          return bob.build();
        }
        finally {
          for (Aggregator agg : aggregators) {
            agg.close();
          }
        }
      }
    }

    @Override
    protected void close(OperatorProfile profile)
    {
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount());
      profile.add(OperatorProfile.CURSOR_COUNT_METRIC, cursorCount);
      cursorIter.close();
    }
  }

  /**
   * Vectorized result iterator. The cursor provides a vector cursor. The client is obligated
   * to do time-grouping, which is done via several layers of abstractions.
   */
  public static class VectorizedResultIterator extends BaseResultIterator
  {
    private final TimeGroupVectorIterator groupIter;
    private final Closer closer;
    private final AggregatorAdapters aggregators;
    private final ByteBuffer buffer;
    private int groupCount;
    private int batchCount;

    public VectorizedResultIterator(
        final CursorDefinition cursorDefinition,
        final VectorCursor cursor,
        final boolean skipEmptyBuckets,
        final NonBlockingPool<ByteBuffer> bufferPool,
        final List<AggregatorFactory> aggregatorSpecs,
        final Closer closer
    )
    {
      super(
          aggregatorSpecs,
          skipEmptyBuckets
      );
      this.groupIter = new TimeGroupVectorIterator(
          cursorDefinition.adapter,
          cursor,
          cursorDefinition.granularity,
          cursorDefinition.queryInterval,
          !skipEmptyBuckets
      );
      this.closer = closer;

      final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
      aggregators = closer.register(
          AggregatorAdapters.factorizeVector(columnSelectorFactory, aggregatorSpecs)
      );

      final ResourceHolder<ByteBuffer> bufferHolder = closer.register(bufferPool.take());
      this.buffer = bufferHolder.get();

      if (aggregators.spaceNeeded() > buffer.remaining()) {
        Operators.closeSafely(closer);
        throw new ISE(
            "Not enough space for aggregators, needed [%,d] bytes but have only [%,d].",
            aggregators.spaceNeeded(),
            buffer.remaining()
        );
      }
    }

    @Override
    public Result<TimeseriesResultValue> next() throws ResultIterator.EofException
    {
      if (!groupIter.nextGroup()) {
        throw Operators.eof();
      }
      groupCount++;

      // Aggregate totals for the bucket.
      aggregators.init(buffer, 0);
      while (groupIter.nextBatch()) {
        if (!groupIter.batchIsEmpty()) {
          aggregators.aggregateVector(
              buffer,
              0,
              groupIter.startOffset(),
              groupIter.endOffset()
          );
          batchCount++;
          rowCount += groupIter.endOffset() - groupIter.startOffset();
        }
      }

      // Convert the values to the output form.
      final TimeseriesResultBuilder bob = new TimeseriesResultBuilder(
          groupIter.granularity().toDateTime(groupIter.group().getStartMillis())
      );
      for (int i = 0; i < aggregatorSpecs.size(); i++) {
        bob.addMetric(
            aggregatorSpecs.get(i).getName(),
            aggregators.get(buffer, 0, i)
        );
      }

      return bob.build();
    }

    @Override
    protected void close(OperatorProfile profile)
    {
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount());
      profile.add("group-count", groupCount);
      profile.add("batch-count", batchCount);
      Operators.closeSafely(closer);
    }
  }

  private final FragmentContext context;
  private final CursorDefinition cursorDefinition;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final boolean skipEmptyBuckets;
  private State state = State.START;
  private BaseResultIterator resultIter;

  public TimeseriesEngineOperator(
      final FragmentContext context,
      final CursorDefinition cursorDefinition,
      final List<AggregatorFactory> aggregatorSpecs,
      final NonBlockingPool<ByteBuffer> bufferPool,
      final boolean skipEmptyBuckets
  )
  {
    this.context = context;
    this.cursorDefinition = cursorDefinition;
    this.aggregatorSpecs = aggregatorSpecs;
    this.bufferPool = bufferPool;
    this.skipEmptyBuckets = skipEmptyBuckets;
    context.register(this);
  }

  private boolean isVectorized()
  {
    return bufferPool != null;
  }

  @Override
  public ResultIterator<Result<TimeseriesResultValue>> open()
  {
    state = State.RUN;
    if (isVectorized()) {
      return makeVectorizedIterator();
    } else {
      return makeNonVectorizedIterator();
    }
  }

  private ResultIterator<Result<TimeseriesResultValue>> makeVectorizedIterator()
  {
    final Closer closer = Closer.create();
    try {
      final VectorCursor cursor = cursorDefinition.vectorCursor();
      if (cursor == null) {
        return Iterators.emptyIterator();
      }
      closer.register(cursor);

      return new VectorizedResultIterator(
          cursorDefinition,
          cursor,
          skipEmptyBuckets,
          bufferPool,
          aggregatorSpecs,
          closer
      );
    }
    catch (RuntimeException t1) {
      try {
        closer.close();
      }
      catch (Throwable t2) {
        t1.addSuppressed(t2);
      }
      throw t1;
    }
  }

  private BaseResultIterator makeNonVectorizedIterator()
  {
    return new NonVectorizedResultIterator(
        cursorDefinition.cursors(),
        aggregatorSpecs,
        skipEmptyBuckets
    );
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN) {
      OperatorProfile profile = new OperatorProfile("timeseries-engine");
      profile.add(OperatorProfile.VECTORIZED_ATTRIB, isVectorized());
      if (resultIter == null) {
        profile.add(OperatorProfile.ROW_COUNT_METRIC, 0);
      } else {
        resultIter.close(profile);
      }
      context.updateProfile(this, profile);
    }
    resultIter = null;
    state = State.CLOSED;
  }
}
