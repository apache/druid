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

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.ImmutableSortedSet;
import org.apache.druid.com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests the order in which Scan query results come back.
 *
 * Ensures that we have run-to-run stability of result order, which is important for offset-based pagination.
 */
@RunWith(Parameterized.class)
public class ScanQueryResultOrderingTest
{
  private static final String DATASOURCE = "datasource";
  private static final String ID_COLUMN = "id";

  private static final RowAdapter<Object[]> ROW_ADAPTER = new RowAdapter<Object[]>()
  {
    @Override
    public ToLongFunction<Object[]> timestampFunction()
    {
      return row -> ((DateTime) row[0]).getMillis();
    }

    @Override
    public Function<Object[], Object> columnFunction(String columnName)
    {
      if (ID_COLUMN.equals(columnName)) {
        return row -> row[1];
      } else if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
        return timestampFunction()::applyAsLong;
      } else {
        return row -> null;
      }
    }
  };

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .addTimeColumn()
                                                                .add(ID_COLUMN, ValueType.LONG)
                                                                .build();

  private static final List<RowBasedSegment<Object[]>> SEGMENTS = ImmutableList.of(
      new RowBasedSegment<>(
          SegmentId.of(DATASOURCE, Intervals.of("2000-01-01/P1D"), "1", 0),
          ImmutableList.of(
              new Object[]{DateTimes.of("2000T01"), 101},
              new Object[]{DateTimes.of("2000T01"), 80},
              new Object[]{DateTimes.of("2000T01"), 232},
              new Object[]{DateTimes.of("2000T01"), 12},
              new Object[]{DateTimes.of("2000T02"), 808},
              new Object[]{DateTimes.of("2000T02"), 411},
              new Object[]{DateTimes.of("2000T02"), 383},
              new Object[]{DateTimes.of("2000T05"), 22}
          ),
          ROW_ADAPTER,
          ROW_SIGNATURE
      ),
      new RowBasedSegment<>(
          SegmentId.of(DATASOURCE, Intervals.of("2000-01-01/P1D"), "1", 1),
          ImmutableList.of(
              new Object[]{DateTimes.of("2000T01"), 333},
              new Object[]{DateTimes.of("2000T01"), 222},
              new Object[]{DateTimes.of("2000T01"), 444},
              new Object[]{DateTimes.of("2000T01"), 111},
              new Object[]{DateTimes.of("2000T03"), 555},
              new Object[]{DateTimes.of("2000T03"), 999},
              new Object[]{DateTimes.of("2000T03"), 888},
              new Object[]{DateTimes.of("2000T05"), 777}
          ),
          ROW_ADAPTER,
          ROW_SIGNATURE
      ),
      new RowBasedSegment<>(
          SegmentId.of(DATASOURCE, Intervals.of("2000-01-02/P1D"), "1", 0),
          ImmutableList.of(
              new Object[]{DateTimes.of("2000-01-02T00"), 7},
              new Object[]{DateTimes.of("2000-01-02T02"), 9},
              new Object[]{DateTimes.of("2000-01-02T03"), 8}
          ),
          ROW_ADAPTER,
          ROW_SIGNATURE
      )
  );

  private final List<Integer> segmentToServerMap;
  private final int limit;
  private final int batchSize;
  private final int maxRowsQueuedForOrdering;

  private ScanQueryRunnerFactory queryRunnerFactory;
  private List<QueryRunner<ScanResultValue>> segmentRunners;

  @Parameterized.Parameters(name = "Segment-to-server map[{0}], limit[{1}], batchSize[{2}], maxRowsQueuedForOrdering[{3}]")
  public static Iterable<Object[]> constructorFeeder()
  {
    // Set number of server equal to number of segments, then try all possible distributions of segments to servers.
    final int numServers = SEGMENTS.size();

    final Set<List<Integer>> segmentToServerMaps = Sets.cartesianProduct(
        IntStream.range(0, SEGMENTS.size())
                 .mapToObj(i -> IntStream.range(0, numServers).boxed().collect(Collectors.toSet()))
                 .collect(Collectors.toList())
    );

    // Try every limit up to one past the total number of rows.
    final Set<Integer> limits = new TreeSet<>();
    final int totalNumRows = SEGMENTS.stream().mapToInt(s -> s.asStorageAdapter().getNumRows()).sum();
    for (int i = 0; i <= totalNumRows + 1; i++) {
      limits.add(i);
    }

    // Try various batch sizes.
    final Set<Integer> batchSizes = ImmutableSortedSet.of(1, 2, 100);
    final Set<Integer> maxRowsQueuedForOrderings = ImmutableSortedSet.of(1, 7, 100000);

    return Sets.cartesianProduct(
        segmentToServerMaps,
        limits,
        batchSizes,
        maxRowsQueuedForOrderings
    ).stream().map(args -> args.toArray(new Object[0])).collect(Collectors.toList());
  }

  public ScanQueryResultOrderingTest(
      final List<Integer> segmentToServerMap,
      final int limit,
      final int batchSize,
      final int maxRowsQueuedForOrdering
  )
  {
    this.segmentToServerMap = segmentToServerMap;
    this.limit = limit;
    this.batchSize = batchSize;
    this.maxRowsQueuedForOrdering = maxRowsQueuedForOrdering;
  }

  @Before
  public void setUp()
  {
    queryRunnerFactory = new ScanQueryRunnerFactory(
        new ScanQueryQueryToolChest(
            new ScanQueryConfig(),
            new DefaultGenericQueryMetricsFactory()
        ),
        new ScanQueryEngine(),
        new ScanQueryConfig()
    );

    segmentRunners = SEGMENTS.stream().map(queryRunnerFactory::createRunner).collect(Collectors.toList());
  }

  @Test
  public void testOrderNone()
  {
    assertResultsEquals(
        Druids.newScanQueryBuilder()
              .dataSource("ds")
              .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2000/P1D"))))
              .columns(ColumnHolder.TIME_COLUMN_NAME, ID_COLUMN)
              .order(ScanQuery.Order.NONE)
              .build(),
        ImmutableList.of(
            101,
            80,
            232,
            12,
            808,
            411,
            383,
            22,
            333,
            222,
            444,
            111,
            555,
            999,
            888,
            777,
            7,
            9,
            8
        )
    );
  }

  @Test
  public void testOrderTimeAscending()
  {
    assertResultsEquals(
        Druids.newScanQueryBuilder()
              .dataSource("ds")
              .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2000/P1D"))))
              .columns(ColumnHolder.TIME_COLUMN_NAME, ID_COLUMN)
              .order(ScanQuery.Order.ASCENDING)
              .build(),
        ImmutableList.of(
            101,
            80,
            232,
            12,
            333,
            222,
            444,
            111,
            808,
            411,
            383,
            555,
            999,
            888,
            22,
            777,
            7,
            9,
            8
        )
    );
  }

  @Test
  public void testOrderTimeDescending()
  {
    assertResultsEquals(
        Druids.newScanQueryBuilder()
              .dataSource("ds")
              .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2000/P1D"))))
              .columns(ColumnHolder.TIME_COLUMN_NAME, ID_COLUMN)
              .order(ScanQuery.Order.DESCENDING)
              .build(),
        ImmutableList.of(
            8,
            9,
            7,
            777,
            22,
            888,
            999,
            555,
            383,
            411,
            808,
            111,
            444,
            222,
            333,
            12,
            232,
            80,
            101
        )
    );
  }

  private void assertResultsEquals(final ScanQuery query, final List<Integer> expectedResults)
  {
    final List<List<Pair<SegmentId, QueryRunner<ScanResultValue>>>> serverRunners = new ArrayList<>();
    for (int i = 0; i <= segmentToServerMap.stream().max(Comparator.naturalOrder()).orElse(0); i++) {
      serverRunners.add(new ArrayList<>());
    }

    for (int segmentNumber = 0; segmentNumber < segmentToServerMap.size(); segmentNumber++) {
      final SegmentId segmentId = SEGMENTS.get(segmentNumber).getId();
      final int serverNumber = segmentToServerMap.get(segmentNumber);

      serverRunners.get(serverNumber).add(Pair.of(segmentId, segmentRunners.get(segmentNumber)));
    }

    // Simulates what the Historical servers would do.
    final List<QueryRunner<ScanResultValue>> mergedServerRunners =
        serverRunners.stream()
                     .filter(runners -> !runners.isEmpty())
                     .map(
                         runners ->
                             queryRunnerFactory.getToolchest().mergeResults(
                                 new QueryRunner<ScanResultValue>()
                                 {
                                   @Override
                                   public Sequence<ScanResultValue> run(
                                       final QueryPlus<ScanResultValue> queryPlus,
                                       final ResponseContext responseContext
                                   )
                                   {
                                     return queryRunnerFactory.mergeRunners(
                                         Execs.directExecutor(),
                                         runners.stream().map(p -> p.rhs).collect(Collectors.toList())
                                     ).run(
                                         queryPlus.withQuery(
                                             queryPlus.getQuery()
                                                      .withQuerySegmentSpec(
                                                          new MultipleSpecificSegmentSpec(
                                                              runners.stream()
                                                                     .map(p -> p.lhs.toDescriptor())
                                                                     .collect(Collectors.toList())
                                                          )
                                                      )
                                         ),
                                         responseContext
                                     );
                                   }
                                 }
                             )
                     )
                     .collect(Collectors.toList());

    // Simulates what the Broker would do.
    final QueryRunner<ScanResultValue> brokerRunner = queryRunnerFactory.getToolchest().mergeResults(
        (queryPlus, responseContext) -> {
          final List<Sequence<ScanResultValue>> sequences =
              mergedServerRunners.stream()
                                 .map(runner -> runner.run(queryPlus.withoutThreadUnsafeState()))
                                 .collect(Collectors.toList());

          return new MergeSequence<>(
              queryPlus.getQuery().getResultOrdering(),
              Sequences.simple(sequences)
          );
        }
    );

    // Finally: run the query.
    final List<Integer> results = runQuery(
        (ScanQuery) Druids.ScanQueryBuilder.copy(query)
                                           .limit(limit)
                                           .batchSize(batchSize)
                                           .build()
                                           .withOverriddenContext(
                                               ImmutableMap.of(
                                                   ScanQueryConfig.CTX_KEY_MAX_ROWS_QUEUED_FOR_ORDERING,
                                                   maxRowsQueuedForOrdering
                                               )
                                           ),
        brokerRunner
    );

    Assert.assertEquals(
        expectedResults.stream().limit(limit == 0 ? Long.MAX_VALUE : limit).collect(Collectors.toList()),
        results
    );
  }

  private List<Integer> runQuery(final ScanQuery query, final QueryRunner<ScanResultValue> brokerRunner)
  {
    final List<Object[]> results = queryRunnerFactory.getToolchest().resultsAsArrays(
        query,
        brokerRunner.run(QueryPlus.wrap(query))
    ).toList();

    return results.stream().mapToInt(row -> (int) row[1]).boxed().collect(Collectors.toList());
  }
}
