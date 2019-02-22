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

package org.apache.druid.query.scan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.segment.Segment;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

public class ScanQueryRunnerFactory implements QueryRunnerFactory<ScanResultValue, ScanQuery>
{
  // This variable indicates when a running query should be expired,
  // and is effective only when 'timeout' of queryContext has a positive value.
  public static final String CTX_TIMEOUT_AT = "timeoutAt";
  public static final String CTX_COUNT = "count";
  private final ScanQueryQueryToolChest toolChest;
  private final ScanQueryEngine engine;
  private final ScanQueryConfig scanQueryConfig;

  @Inject
  public ScanQueryRunnerFactory(
      ScanQueryQueryToolChest toolChest,
      ScanQueryEngine engine,
      ScanQueryConfig scanQueryConfig
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.scanQueryConfig = scanQueryConfig;
  }

  @Override
  public QueryRunner<ScanResultValue> createRunner(Segment segment)
  {
    return new ScanQueryRunner(engine, segment);
  }

  @Override
  public QueryRunner<ScanResultValue> mergeRunners(
      ExecutorService queryExecutor,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners
  )
  {
    // in single thread and in jetty thread instead of processing thread
    return (queryPlus, responseContext) -> {
      ScanQuery query = (ScanQuery) queryPlus.getQuery();
      int numSegments = 0;
      final Iterator<QueryRunner<ScanResultValue>> segmentIt = queryRunners.iterator();
      for (; segmentIt.hasNext(); numSegments++) {
        segmentIt.next();
      }
      // Note: this variable is effective only when queryContext has a timeout.
      // See the comment of CTX_TIMEOUT_AT.
      final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
      responseContext.put(CTX_TIMEOUT_AT, timeoutAt);
      if (query.getTimeOrder().equals(ScanQuery.TimeOrder.NONE)) {
        // Use normal strategy
        return Sequences.concat(
            Sequences.map(
                Sequences.simple(queryRunners),
                input -> input.run(queryPlus, responseContext)
            )
        );
      } else if (query.getLimit() <= scanQueryConfig.getMaxRowsQueuedForTimeOrdering()) {
        // Use priority queue strategy
        Sequence<ScanResultValue> queryResults = Sequences.concat(Sequences.map(
            Sequences.simple(queryRunners),
            input -> input.run(queryPlus, responseContext)
        ));
        return sortBatchAndLimitScanResultValues(queryResults, query);
      } else if (numSegments <= scanQueryConfig.getMaxSegmentsTimeOrderedInMemory()) {
        // Use flatMerge strategy
        return Sequences.map(
            Sequences.simple(queryRunners),
            (input) -> Sequences.concat(
                Sequences.map(
                    input.run(queryPlus, responseContext),
                    srv -> Sequences.simple(srv.toSingleEventScanResultValues())
                )
            )
        ).flatMerge(
            seq -> seq,
            Ordering.from(new ScanResultValueTimestampComparator(
                query
            ))
        ).limit(
            Math.toIntExact(query.getLimit())
        );
      } else {
        throw new UOE(
            "Time ordering for result set limit of %,d is not supported.  Try lowering the "
            + "result set size to less than or equal to the time ordering limit of %,d.",
            query.getLimit(),
            scanQueryConfig.getMaxRowsQueuedForTimeOrdering()
        );
      }
    };
  }

  @VisibleForTesting
  Sequence<ScanResultValue> sortBatchAndLimitScanResultValues(
      Sequence<ScanResultValue> inputSequence,
      ScanQuery scanQuery
  )
  {
    Comparator<ScanResultValue> priorityQComparator =
        new ScanResultValueTimestampComparator(scanQuery);

    // Converting the limit from long to int could theoretically throw an ArithmeticException but this branch
    // only runs if limit < MAX_LIMIT_FOR_IN_MEMORY_TIME_ORDERING (which should be < Integer.MAX_VALUE)
    int limit = Math.toIntExact(scanQuery.getLimit());
    PriorityQueue<ScanResultValue> q = new PriorityQueue<>(limit, priorityQComparator);

    Yielder<ScanResultValue> yielder = inputSequence.toYielder(
        null,
        new YieldingAccumulator<ScanResultValue, ScanResultValue>()
        {
          @Override
          public ScanResultValue accumulate(ScanResultValue accumulated, ScanResultValue in)
          {
            yield();
            return in;
          }
        }
    );
    while (!yielder.isDone()) {
      ScanResultValue next = yielder.get();
      List<ScanResultValue> singleEventScanResultValues = next.toSingleEventScanResultValues();
      for (ScanResultValue srv : singleEventScanResultValues) {
        // Using an intermediate unbatched ScanResultValue is not that great memory-wise, but the column list
        // needs to be preserved for queries using the compactedList result format
        q.offer(srv);
        if (q.size() > limit) {
          q.poll();
        }
      }
      yielder = yielder.next(null);
    }
    // Need to convert to a List because Priority Queue's iterator doesn't guarantee that the sorted order
    // will be maintained
    final Deque<ScanResultValue> sortedElements = new ArrayDeque<>(q.size());
    while (q.size() != 0) {
      // We add at the front of the list because poll removes the tail of the queue.
      sortedElements.addFirst(q.poll());
    }

    // We can use an iterator here because all the results have been materialized for sorting

    return new BaseSequence(
        new BaseSequence.IteratorMaker<ScanResultValue, ScanBatchedIterator>()
        {
          @Override
          public ScanBatchedIterator make()
          {
            return new ScanBatchedIterator(
                sortedElements.iterator(),
                scanQuery.getBatchSize()
            );
          }

          @Override
          public void cleanup(ScanBatchedIterator iterFromMake)
          {
            CloseQuietly.close(iterFromMake);
          }
        });
  }

  @Override
  public QueryToolChest<ScanResultValue, ScanQuery> getToolchest()
  {
    return toolChest;
  }

  private static class ScanQueryRunner implements QueryRunner<ScanResultValue>
  {
    private final ScanQueryEngine engine;
    private final Segment segment;

    public ScanQueryRunner(ScanQueryEngine engine, Segment segment)
    {
      this.engine = engine;
      this.segment = segment;
    }

    @Override
    public Sequence<ScanResultValue> run(QueryPlus<ScanResultValue> queryPlus, Map<String, Object> responseContext)
    {
      Query<ScanResultValue> query = queryPlus.getQuery();
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }

      // it happens in unit tests
      final Number timeoutAt = (Number) responseContext.get(CTX_TIMEOUT_AT);
      if (timeoutAt == null || timeoutAt.longValue() == 0L) {
        responseContext.put(CTX_TIMEOUT_AT, JodaUtils.MAX_INSTANT);
      }
      return engine.process((ScanQuery) query, segment, responseContext);
    }
  }
  /**
   * This iterator supports iteration through any Iterable of unbatched ScanResultValues (1 event/ScanResultValue) and
   * aggregates events into ScanResultValues with {@code batchSize} events.  The columns from the first event per
   * ScanResultValue will be used to populate the column section.
   */
  private static class ScanBatchedIterator implements CloseableIterator<ScanResultValue>
  {
    private final Iterator<ScanResultValue> itr;
    private final int batchSize;

    public ScanBatchedIterator(Iterator<ScanResultValue> iterator, int batchSize)
    {
      this.itr = iterator;
      this.batchSize = batchSize;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public boolean hasNext()
    {
      return itr.hasNext();
    }

    @Override
    public ScanResultValue next()
    {
      // Create new ScanResultValue from event map
      List<Object> eventsToAdd = new ArrayList<>(batchSize);
      List<String> columns = new ArrayList<>();
      while (eventsToAdd.size() < batchSize && itr.hasNext()) {
        ScanResultValue srv = itr.next();
        // Only replace once using the columns from the first event
        columns = columns.isEmpty() ? srv.getColumns() : columns;
        eventsToAdd.add(Iterables.getOnlyElement((List) srv.getEvents()));
      }
      return new ScanResultValue(null, columns, eventsToAdd);
    }
  }
}
