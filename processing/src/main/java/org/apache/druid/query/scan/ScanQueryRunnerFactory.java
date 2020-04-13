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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SinkQueryRunners;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.Segment;
import org.joda.time.Interval;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class ScanQueryRunnerFactory implements QueryRunnerFactory<ScanResultValue, ScanQuery>
{
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

      // Note: this variable is effective only when queryContext has a timeout.
      // See the comment of ResponseContext.Key.TIMEOUT_AT.
      final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
      responseContext.put(ResponseContext.Key.TIMEOUT_AT, timeoutAt);

      if (query.getOrder().equals(ScanQuery.Order.NONE)) {
        // Use normal strategy
        Sequence<ScanResultValue> returnedRows = Sequences.concat(
            Sequences.map(
                Sequences.simple(queryRunners),
                input -> input.run(queryPlus, responseContext)
            )
        );
        if (query.getScanRowsLimit() <= Integer.MAX_VALUE) {
          return returnedRows.limit(Math.toIntExact(query.getScanRowsLimit()));
        } else {
          return returnedRows;
        }
      } else {
        List<Interval> intervalsOrdered = getIntervalsFromSpecificQuerySpec(query.getQuerySegmentSpec());
        List<QueryRunner<ScanResultValue>> queryRunnersOrdered = Lists.newArrayList(queryRunners);

        if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
          intervalsOrdered = Lists.reverse(intervalsOrdered);
          queryRunnersOrdered = Lists.reverse(queryRunnersOrdered);
        }
        int maxRowsQueuedForOrdering = (query.getMaxRowsQueuedForOrdering() == null
                                        ? scanQueryConfig.getMaxRowsQueuedForOrdering()
                                        : query.getMaxRowsQueuedForOrdering());
        if (query.getScanRowsLimit() <= maxRowsQueuedForOrdering) {
          // Use priority queue strategy
          return priorityQueueSortAndLimit(
              Sequences.concat(Sequences.map(
                  Sequences.simple(queryRunnersOrdered),
                  input -> input.run(queryPlus, responseContext)
              )),
              query,
              intervalsOrdered
          );
        } else {
          // Use n-way merge strategy
          List<Pair<Interval, QueryRunner<ScanResultValue>>> intervalsAndRunnersOrdered = new ArrayList<>();
          if (intervalsOrdered.size() == queryRunnersOrdered.size()) {
            for (int i = 0; i < queryRunnersOrdered.size(); i++) {
              intervalsAndRunnersOrdered.add(new Pair<>(intervalsOrdered.get(i), queryRunnersOrdered.get(i)));
            }
          } else if (queryRunners instanceof SinkQueryRunners) {
            ((SinkQueryRunners<ScanResultValue>) queryRunners).runnerIntervalMappingIterator()
                                                              .forEachRemaining(intervalsAndRunnersOrdered::add);
          } else {
            throw new ISE("Number of segment descriptors does not equal number of "
                          + "query runners...something went wrong!");
          }

          // Group the list of pairs by interval.  The LinkedHashMap will have an interval paired with a list of all the
          // query runners for that segment
          LinkedHashMap<Interval, List<Pair<Interval, QueryRunner<ScanResultValue>>>> partitionsGroupedByInterval =
              intervalsAndRunnersOrdered.stream()
                                          .collect(Collectors.groupingBy(
                                              x -> x.lhs,
                                              LinkedHashMap::new,
                                              Collectors.toList()
                                          ));

          // Find the segment with the largest numbers of partitions.  This will be used to compare with the
          // maxSegmentPartitionsOrderedInMemory limit to determine if the query is at risk of consuming too much memory.
          int maxNumPartitionsInSegment =
              partitionsGroupedByInterval.values()
                                         .stream()
                                         .map(x -> x.size())
                                         .max(Comparator.comparing(Integer::valueOf))
                                         .get();

          int segmentPartitionLimit = query.getMaxSegmentPartitionsOrderedInMemory() == null
                                      ? scanQueryConfig.getMaxSegmentPartitionsOrderedInMemory()
                                      : query.getMaxSegmentPartitionsOrderedInMemory();
          if (maxNumPartitionsInSegment <= segmentPartitionLimit) {
            // Use n-way merge strategy

            // Create a list of grouped runner lists (i.e. each sublist/"runner group" corresponds to an interval) ->
            // there should be no interval overlap.  We create a list of lists so we can create a sequence of sequences.
            // There's no easy way to convert a LinkedHashMap to a sequence because it's non-iterable.
            List<List<QueryRunner<ScanResultValue>>> groupedRunners =
                partitionsGroupedByInterval.entrySet()
                                           .stream()
                                           .map(entry -> entry.getValue()
                                                              .stream()
                                                              .map(segQueryRunnerPair -> segQueryRunnerPair.rhs)
                                                              .collect(Collectors.toList()))
                                           .collect(Collectors.toList());

            return nWayMergeAndLimit(groupedRunners, queryPlus, responseContext);
          }
          throw new UOE(
              "Time ordering for queries of %,d partitions per segment and a row limit of %,d is not supported."
              + "  Try reducing the scope of the query to scan fewer partitions than the configurable limit of"
              + " %,d partitions or lower the row limit below %,d.",
              maxNumPartitionsInSegment,
              query.getScanRowsLimit(),
              scanQueryConfig.getMaxSegmentPartitionsOrderedInMemory(),
              scanQueryConfig.getMaxRowsQueuedForOrdering()
          );
        }
      }
    };
  }

  @VisibleForTesting
  Sequence<ScanResultValue> priorityQueueSortAndLimit(
      Sequence<ScanResultValue> inputSequence,
      ScanQuery scanQuery,
      List<Interval> intervalsOrdered
  )
  {
    Comparator<ScanResultValue> priorityQComparator = new ScanResultValueTimestampComparator(scanQuery);

    if (scanQuery.getScanRowsLimit() > Integer.MAX_VALUE) {
      throw new UOE(
          "Limit of %,d rows not supported for priority queue strategy of time-ordering scan results",
          scanQuery.getScanRowsLimit()
      );
    }

    // Converting the limit from long to int could theoretically throw an ArithmeticException but this branch
    // only runs if limit < MAX_LIMIT_FOR_IN_MEMORY_TIME_ORDERING (which should be < Integer.MAX_VALUE)
    int limit = Math.toIntExact(scanQuery.getScanRowsLimit());

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
    boolean doneScanning = yielder.isDone();
    // We need to scan limit elements and anything else in the last segment
    int numRowsScanned = 0;
    Interval finalInterval = null;
    while (!doneScanning) {
      ScanResultValue next = yielder.get();
      List<ScanResultValue> singleEventScanResultValues = next.toSingleEventScanResultValues();
      for (ScanResultValue srv : singleEventScanResultValues) {
        numRowsScanned++;
        // Using an intermediate unbatched ScanResultValue is not that great memory-wise, but the column list
        // needs to be preserved for queries using the compactedList result format
        q.offer(srv);
        if (q.size() > limit) {
          q.poll();
        }

        // Finish scanning the interval containing the limit row
        if (numRowsScanned > limit && finalInterval == null) {
          long timestampOfLimitRow = srv.getFirstEventTimestamp(scanQuery.getResultFormat());
          for (Interval interval : intervalsOrdered) {
            if (interval.contains(timestampOfLimitRow)) {
              finalInterval = interval;
            }
          }
          if (finalInterval == null) {
            throw new ISE("WTH???  Row came from an unscanned interval?");
          }
        }
      }
      yielder = yielder.next(null);
      doneScanning = yielder.isDone() ||
                     (finalInterval != null &&
                      !finalInterval.contains(next.getFirstEventTimestamp(scanQuery.getResultFormat())));
    }
    // Need to convert to a Deque because Priority Queue's iterator doesn't guarantee that the sorted order
    // will be maintained.  Deque was chosen over list because its addFirst is O(1).
    final Deque<ScanResultValue> sortedElements = new ArrayDeque<>(q.size());
    while (q.size() != 0) {
      // addFirst is used since PriorityQueue#poll() dequeues the low-priority (timestamp-wise) events first.
      sortedElements.addFirst(q.poll());
    }
    return Sequences.simple(sortedElements);
  }

  @VisibleForTesting
  List<Interval> getIntervalsFromSpecificQuerySpec(QuerySegmentSpec spec)
  {
    // Query segment spec must be an instance of MultipleSpecificSegmentSpec or SpecificSegmentSpec because
    // segment descriptors need to be present for a 1:1 matching of intervals with query runners.
    // The other types of segment spec condense the intervals (i.e. merge neighbouring intervals), eliminating
    // the 1:1 relationship between intervals and query runners.
    List<Interval> descriptorsOrdered;

    if (spec instanceof MultipleSpecificSegmentSpec) {
      // Ascending time order for both descriptors and query runners by default
      descriptorsOrdered = ((MultipleSpecificSegmentSpec) spec).getDescriptors()
                                                               .stream()
                                                               .map(SegmentDescriptor::getInterval)
                                                               .collect(Collectors.toList());
    } else if (spec instanceof SpecificSegmentSpec) {
      descriptorsOrdered = Collections.singletonList(((SpecificSegmentSpec) spec).getDescriptor().getInterval());
    } else {
      throw new UOE(
          "Time-ordering on scan queries is only supported for queries with segment specs "
          + "of type MultipleSpecificSegmentSpec or SpecificSegmentSpec...a [%s] was received instead.",
          spec.getClass().getSimpleName()
      );
    }
    return descriptorsOrdered;
  }

  @VisibleForTesting
  Sequence<ScanResultValue> nWayMergeAndLimit(
      List<List<QueryRunner<ScanResultValue>>> groupedRunners,
      QueryPlus<ScanResultValue> queryPlus,
      ResponseContext responseContext
  )
  {
    // Starting from the innermost Sequences.map:
    // (1) Deaggregate each ScanResultValue returned by the query runners
    // (2) Combine the deaggregated ScanResultValues into a single sequence
    // (3) Create a sequence of results from each runner in the group and flatmerge based on timestamp
    // (4) Create a sequence of results from each runner group
    // (5) Join all the results into a single sequence
    Sequence<ScanResultValue> resultSequence =
        Sequences.concat(
            Sequences.map(
                Sequences.simple(groupedRunners),
                runnerGroup ->
                    Sequences.map(
                        Sequences.simple(runnerGroup),
                        (input) -> Sequences.concat(
                            Sequences.map(
                                input.run(queryPlus, responseContext),
                                srv -> Sequences.simple(srv.toSingleEventScanResultValues())
                            )
                        )
                    ).flatMerge(
                        seq -> seq,
                        Ordering.from(new ScanResultValueTimestampComparator(
                            (ScanQuery) queryPlus.getQuery()
                        )).reverse()
                    )
            )
        );
    long limit = ((ScanQuery) (queryPlus.getQuery())).getScanRowsLimit();
    if (limit == Long.MAX_VALUE) {
      return resultSequence;
    }
    return resultSequence.limit(limit);
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
    public Sequence<ScanResultValue> run(QueryPlus<ScanResultValue> queryPlus, ResponseContext responseContext)
    {
      Query<ScanResultValue> query = queryPlus.getQuery();
      if (!(query instanceof ScanQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
      }

      // it happens in unit tests
      final Number timeoutAt = (Number) responseContext.get(ResponseContext.Key.TIMEOUT_AT);
      if (timeoutAt == null || timeoutAt.longValue() == 0L) {
        responseContext.put(ResponseContext.Key.TIMEOUT_AT, JodaUtils.MAX_INSTANT);
      }
      return engine.process((ScanQuery) query, segment, responseContext);
    }
  }
}
