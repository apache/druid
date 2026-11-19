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

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SinkQueryRunners;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class ScanQueryRunnerFactoryTest
{
  private static final ScanQueryConfig CONFIG = new ScanQueryConfig()
  {
    @Override
    public int getMaxRowsQueuedForOrdering()
    {
      return 10000;
    }

    @Override
    public int getMaxSegmentPartitionsOrderedInMemory()
    {
      return 4;
    }
  };

  private static final ScanQueryRunnerFactory FACTORY = new ScanQueryRunnerFactory(
      new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
      new ScanQueryEngine(),
      CONFIG
  );

  @ParameterizedClass
  @MethodSource("constructorFeeder")
  public static class ScanQueryRunnerFactoryParameterizedTest
  {
    private int numElements;
    private ScanQuery query;
    private ScanQuery.ResultFormat resultFormat;

    public ScanQueryRunnerFactoryParameterizedTest(
        final int numElements,
        final int batchSize,
        final long limit,
        final ScanQuery.ResultFormat resultFormat,
        final Order order
    )
    {
      this.numElements = numElements;
      this.query = Druids.newScanQueryBuilder()
                         .batchSize(batchSize)
                         .limit(limit)
                         .order(order)
                         .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                         .dataSource("some datasource")
                         .resultFormat(resultFormat)
                         .build();
      this.resultFormat = resultFormat;
    }

    public static Stream<Object[]> constructorFeeder()
    {
      List<Integer> numsElements = ImmutableList.of(0, 10, 100);
      List<Integer> batchSizes = ImmutableList.of(1, 100);
      List<Long> limits = ImmutableList.of(3L, 1000L, Long.MAX_VALUE);
      List<ScanQuery.ResultFormat> resultFormats = ImmutableList.of(
          ScanQuery.ResultFormat.RESULT_FORMAT_LIST,
          ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST
      );
      List<Order> order = ImmutableList.of(
          Order.ASCENDING,
          Order.DESCENDING
      );

      return StreamSupport.stream(
          QueryRunnerTestHelper.cartesian(numsElements, batchSizes, limits, resultFormats, order).spliterator(),
          false
      );
    }

    @Test
    public void testSortAndLimitScanResultValues()
    {
      List<ScanResultValue> srvs = new ArrayList<>(numElements);
      List<Long> expectedEventTimestamps = new ArrayList<>();
      for (int i = 0; i < numElements; i++) {
        long timestamp = DateTimes.of("2015-01-01").plusHours(i).getMillis();
        expectedEventTimestamps.add(timestamp);
        srvs.add(ScanQueryTestHelper.generateScanResultValue(timestamp, resultFormat, 1));
      }
      expectedEventTimestamps.sort((o1, o2) -> {
        int retVal = 0;
        if (o1 > o2) {
          retVal = 1;
        } else if (o1 < o2) {
          retVal = -1;
        }
        if (query.getTimeOrder().equals(Order.DESCENDING)) {
          return retVal * -1;
        }
        return retVal;
      });
      Sequence<ScanResultValue> inputSequence = Sequences.simple(srvs);
      try {
        List<ScanResultValue> output = FACTORY.stableLimitingSort(
            ImmutableList.of(
                Pair.of(
                    new Interval(DateTimes.of("2010-01-01"), DateTimes.of("2019-01-01").plusHours(1)),
                    (queryPlus, responseContext) -> inputSequence
                )
            ),
            QueryPlus.wrap(query),
            ResponseContext.createEmpty()
        ).toList();
        if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
          Assertions.fail("Unsupported exception should have been thrown due to high limit");
        }
        validateSortedOutput(output, expectedEventTimestamps);
      }
      catch (UOE e) {
        if (query.getScanRowsLimit() <= Integer.MAX_VALUE) {
          Assertions.fail("Unsupported operation exception should not have been thrown here");
        }
      }
    }

    @Test
    public void testNWayMerge()
    {
      List<Long> expectedEventTimestamps = new ArrayList<>(numElements * 3);

      List<ScanResultValue> scanResultValues1 = new ArrayList<>(numElements);
      for (int i = 0; i < numElements; i++) {
        long timestamp = DateTimes.of("2015-01-01").plusMinutes(i * 2).getMillis();
        expectedEventTimestamps.add(timestamp);
        scanResultValues1.add(ScanQueryTestHelper.generateScanResultValue(timestamp, resultFormat, 1));
      }

      List<ScanResultValue> scanResultValues2 = new ArrayList<>(numElements);
      for (int i = 0; i < numElements; i++) {
        long timestamp = DateTimes.of("2015-01-01").plusMinutes(i * 2 + 1).getMillis();
        expectedEventTimestamps.add(timestamp);
        scanResultValues2.add(ScanQueryTestHelper.generateScanResultValue(timestamp, resultFormat, 1));
      }

      List<ScanResultValue> scanResultValues3 = new ArrayList<>(numElements);
      for (int i = 0; i < numElements; i++) {
        long timestamp = DateTimes.of("2015-01-02").plusMinutes(i).getMillis();
        expectedEventTimestamps.add(timestamp);
        scanResultValues3.add(ScanQueryTestHelper.generateScanResultValue(timestamp, resultFormat, 1));
      }

      if (query.getTimeOrder() == Order.DESCENDING) {
        Collections.reverse(scanResultValues1);
        Collections.reverse(scanResultValues2);
        Collections.reverse(scanResultValues3);
      }

      QueryRunner<ScanResultValue> runnerSegment1Partition1 =
          (queryPlus, responseContext) -> Sequences.simple(scanResultValues1);

      QueryRunner<ScanResultValue> runnerSegment1Partition2 =
          (queryPlus, responseContext) -> Sequences.simple(scanResultValues2);


      QueryRunner<ScanResultValue> runnerSegment2Partition1 =
          (queryPlus, responseContext) -> Sequences.simple(scanResultValues3);

      QueryRunner<ScanResultValue> runnerSegment2Partition2 =
          (queryPlus, responseContext) -> Sequences.empty();

      List<List<QueryRunner<ScanResultValue>>> groupedRunners = new ArrayList<>(2);

      if (query.getTimeOrder() == Order.DESCENDING) {
        groupedRunners.add(Arrays.asList(runnerSegment2Partition1, runnerSegment2Partition2));
        groupedRunners.add(Arrays.asList(runnerSegment1Partition1, runnerSegment1Partition2));
      } else {
        groupedRunners.add(Arrays.asList(runnerSegment1Partition1, runnerSegment1Partition2));
        groupedRunners.add(Arrays.asList(runnerSegment2Partition1, runnerSegment2Partition2));
      }

      expectedEventTimestamps.sort((o1, o2) -> {
        int retVal = 0;
        if (o1 > o2) {
          retVal = 1;
        } else if (o1 < o2) {
          retVal = -1;
        }
        if (query.getTimeOrder().equals(Order.DESCENDING)) {
          return retVal * -1;
        }
        return retVal;
      });

      List<ScanResultValue> output =
          FACTORY.nWayMergeAndLimit(
              groupedRunners,
              QueryPlus.wrap(query),
              ResponseContext.createEmpty()
          ).toList();

      validateSortedOutput(output, expectedEventTimestamps);
    }

    private void validateSortedOutput(List<ScanResultValue> output, List<Long> expectedEventTimestamps)
    {
      // check each scan result value has one event
      for (ScanResultValue srv : output) {
        if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)) {
          Assertions.assertTrue(ScanQueryTestHelper.getEventsCompactedListResultFormat(srv).size() == 1);
        } else if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)) {
          Assertions.assertTrue(ScanQueryTestHelper.getEventsListResultFormat(srv).size() == 1);
        }
      }

      // check total # of rows <= limit
      Assertions.assertTrue((long) output.size() <= query.getScanRowsLimit());

      // check ordering is correct
      for (int i = 1; i < output.size(); i++) {
        if (query.getTimeOrder().equals(Order.DESCENDING)) {
          Assertions.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) <
                            output.get(i - 1).getFirstEventTimestamp(resultFormat));
        } else {
          Assertions.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) >
                            output.get(i - 1).getFirstEventTimestamp(resultFormat));
        }
      }

      // check the values are correct
      for (int i = 0; (long) i < query.getScanRowsLimit() && i < output.size(); i++) {
        Assertions.assertEquals((long) expectedEventTimestamps.get(i), output.get(i).getFirstEventTimestamp(resultFormat));
      }
    }
  }

  public static class ScanQueryRunnerFactoryNonParameterizedTest
  {
    private static final Interval HOUR_0 = Intervals.of("2000-01-01T00/PT1H");
    private static final Interval HOUR_1 = Intervals.of("2000-01-01T01/PT1H");
    private static final Interval HOUR_2 = Intervals.of("2000-01-01T02/PT1H");

    private SegmentDescriptor descriptor = new SegmentDescriptor(new Interval(
        DateTimes.of("2010-01-01"),
        DateTimes.of("2019-01-01").plusHours(1)
    ), "1", 0);

    @Test
    public void testGetValidIntervalsFromSpec()
    {
      QuerySegmentSpec multiSpecificSpec = new MultipleSpecificSegmentSpec(
          Collections.singletonList(
              descriptor
          )
      );
      QuerySegmentSpec singleSpecificSpec = new SpecificSegmentSpec(descriptor);

      List<Interval> intervals = FACTORY.getIntervalsFromSpecificQuerySpec(multiSpecificSpec);
      Assertions.assertEquals(1, intervals.size());
      Assertions.assertEquals(descriptor.getInterval(), intervals.get(0));

      intervals = FACTORY.getIntervalsFromSpecificQuerySpec(singleSpecificSpec);
      Assertions.assertEquals(1, intervals.size());
      Assertions.assertEquals(descriptor.getInterval(), intervals.get(0));
    }

    @Test
    public void testGetSegmentDescriptorsFromInvalidIntervalSpec()
    {
      QuerySegmentSpec multiIntervalSpec = new MultipleIntervalSegmentSpec(
          Collections.singletonList(
              new Interval(
                  DateTimes.of("2010-01-01"),
                  DateTimes.of("2019-01-01").plusHours(1)
              )
          )
      );
      Assertions.assertThrows(UOE.class, () -> FACTORY.getIntervalsFromSpecificQuerySpec(multiIntervalSpec));
    }

    @Test
    public void testGetSegmentDescriptorsFromInvalidLegacySpec()
    {
      QuerySegmentSpec legacySpec = new LegacySegmentSpec(
          new Interval(
              DateTimes.of("2010-01-01"),
              DateTimes.of("2019-01-01").plusHours(1)
          )
      );
      Assertions.assertThrows(UOE.class, () -> FACTORY.getIntervalsFromSpecificQuerySpec(legacySpec));
    }


    @Test
    public void testMergeRunnersGuardrailsExceeded()
    {
      final QueryRunner<ScanResultValue> runner = FACTORY.mergeRunners(
          Execs.directExecutor(),
          IntStream.range(0, CONFIG.getMaxSegmentPartitionsOrderedInMemory() + 1)
                   .mapToObj(i -> (QueryRunner<ScanResultValue>) (queryPlus, responseContext) -> Sequences.empty())
                   .collect(Collectors.toList())
      );

      ResourceLimitExceededException ex = Assertions.assertThrows(
          ResourceLimitExceededException.class,
          () -> runner.run(
              QueryPlus.wrap(
                  Druids.newScanQueryBuilder()
                        .dataSource("foo")
                        .limit(CONFIG.getMaxRowsQueuedForOrdering() + 1)
                        .intervals(
                            new MultipleSpecificSegmentSpec(
                                IntStream.range(0, CONFIG.getMaxSegmentPartitionsOrderedInMemory() + 1)
                                         .mapToObj(i -> new SegmentDescriptor(Intervals.ETERNITY, "v0", i))
                                         .collect(Collectors.toList())
                            )
                        )
                        .order(Order.ASCENDING)
                        .build()
              ),
              ResponseContext.createEmpty()
          )
      );
      Assertions.assertEquals(
          "Time ordering is not supported for a Scan query with 5 segments per time chunk and a row limit of"
          + " 10,001. Try reducing your query limit below maxRowsQueuedForOrdering (currently 10,000), or using"
          + " compaction to reduce the number of segments per time chunk, or raising"
          + " maxSegmentPartitionsOrderedInMemory (currently 4) above the number of segments you have per time chunk.",
          ex.getMessage()
      );
    }

    @Test
    public void testPriorityQueueDescendingSkipsOlderIntervals()
    {
      final RecordingRunner hour0 = new RecordingRunner(HOUR_0, 10, 20, 30);
      final RecordingRunner hour1 = new RecordingRunner(HOUR_1, 10, 20, 30);
      final RecordingRunner hour2Partition0 = new RecordingRunner(HOUR_2, 10, 20, 30);
      final RecordingRunner hour2Partition1 = new RecordingRunner(HOUR_2, 15, 25, 35);

      Assertions.assertEquals(
          List.of(minute(HOUR_2, 35), minute(HOUR_2, 30), minute(HOUR_2, 25)),
          runMerged(
              Order.DESCENDING,
              3,
              List.of(HOUR_0, HOUR_1, HOUR_2, HOUR_2),
              List.of(hour0, hour1, hour2Partition0, hour2Partition1)
          )
      );
      Assertions.assertEquals(
          List.of(false, false, true, true),
          ranFlags(hour0, hour1, hour2Partition0, hour2Partition1)
      );
    }

    @Test
    public void testPriorityQueueDescendingRunsIntervalsUntilLimitReached()
    {
      final RecordingRunner hour0 = new RecordingRunner(HOUR_0, 10, 20, 30);
      final RecordingRunner hour1 = new RecordingRunner(HOUR_1, 10, 20, 30);
      final RecordingRunner hour2 = new RecordingRunner(HOUR_2, 10, 20);

      Assertions.assertEquals(
          List.of(minute(HOUR_2, 20), minute(HOUR_2, 10), minute(HOUR_1, 30), minute(HOUR_1, 20)),
          runMerged(Order.DESCENDING, 4, List.of(HOUR_0, HOUR_1, HOUR_2), List.of(hour0, hour1, hour2))
      );
      Assertions.assertEquals(List.of(false, true, true), ranFlags(hour0, hour1, hour2));
    }

    @Test
    public void testPriorityQueueAscendingSkipsLaterIntervals()
    {
      final RecordingRunner hour0Partition0 = new RecordingRunner(HOUR_0, 10, 20, 30);
      final RecordingRunner hour0Partition1 = new RecordingRunner(HOUR_0, 15, 25, 35);
      final RecordingRunner hour1 = new RecordingRunner(HOUR_1, 10, 20, 30);
      final RecordingRunner hour2 = new RecordingRunner(HOUR_2, 10, 20, 30);

      Assertions.assertEquals(
          List.of(minute(HOUR_0, 10), minute(HOUR_0, 15), minute(HOUR_0, 20)),
          runMerged(
              Order.ASCENDING,
              3,
              List.of(HOUR_0, HOUR_0, HOUR_1, HOUR_2),
              List.of(hour0Partition0, hour0Partition1, hour1, hour2)
          )
      );
      Assertions.assertEquals(
          List.of(true, true, false, false),
          ranFlags(hour0Partition0, hour0Partition1, hour1, hour2)
      );
    }

    @Test
    public void testPriorityQueueAscendingNarrowsLaterRunners()
    {
      final RecordingRunner hour0Partition0 = new RecordingRunner(HOUR_0, 10, 20, 30);
      final RecordingRunner hour0Partition1 = new RecordingRunner(HOUR_0, 5, 15, 25, 35);
      final RecordingRunner hour1 = new RecordingRunner(HOUR_1, 10);

      Assertions.assertEquals(
          List.of(minute(HOUR_0, 5), minute(HOUR_0, 10), minute(HOUR_0, 15)),
          runMerged(
              Order.ASCENDING,
              3,
              List.of(HOUR_0, HOUR_0, HOUR_1),
              List.of(hour0Partition0, hour0Partition1, hour1)
          )
      );

      // The second partition reads only up to the last row of the first, inclusive.
      Assertions.assertEquals(List.of(Intervals.of("2000-01-01T00/2000-01-01T02")), hour0Partition0.query.getIntervals());
      Assertions.assertEquals(
          List.of(Intervals.utc(HOUR_0.getStartMillis(), minute(HOUR_0, 30) + 1)),
          hour0Partition1.query.getIntervals()
      );
      Assertions.assertNull(hour1.query);
    }

    @Test
    public void testPriorityQueueDescendingNarrowsLaterRunners()
    {
      final RecordingRunner hour0 = new RecordingRunner(HOUR_0, 50);
      final RecordingRunner hour1Partition0 = new RecordingRunner(HOUR_1, 10, 20, 30);
      final RecordingRunner hour1Partition1 = new RecordingRunner(HOUR_1, 5, 25, 40);

      Assertions.assertEquals(
          List.of(minute(HOUR_1, 40), minute(HOUR_1, 30), minute(HOUR_1, 25)),
          runMerged(
              Order.DESCENDING,
              3,
              List.of(HOUR_0, HOUR_1, HOUR_1),
              List.of(hour0, hour1Partition0, hour1Partition1)
          )
      );

      // Runners run latest first, so the first partition reads only down to the last row of the second, inclusive.
      Assertions.assertEquals(List.of(Intervals.of("2000-01-01T00/2000-01-01T02")), hour1Partition1.query.getIntervals());
      Assertions.assertEquals(
          List.of(Intervals.utc(minute(HOUR_1, 5), HOUR_1.getEndMillis())),
          hour1Partition0.query.getIntervals()
      );
      Assertions.assertNull(hour0.query);
    }

    @Test
    public void testPriorityQueueRetainsEqualTimestampFromLaterRunner()
    {
      // Rows with equal timestamps are ordered by segment ID, so the later runner's row at minute 10 displaces the
      // earlier runner's row at minute 10.
      final RecordingRunner first = new RecordingRunner("b", HOUR_0, 0, 10);
      final RecordingRunner second = new RecordingRunner("a", HOUR_0, 10, 20);

      final List<ScanResultValue> rows =
          runMergedRows(Order.ASCENDING, 2, List.of(HOUR_0, HOUR_0), List.of(first, second));

      Assertions.assertEquals(
          List.of("b", "a"),
          rows.stream().map(ScanResultValue::getSegmentId).collect(Collectors.toList())
      );
      Assertions.assertEquals(List.of(minute(HOUR_0, 0), minute(HOUR_0, 10)), toTimestamps(rows));
      Assertions.assertEquals(
          List.of(Intervals.utc(HOUR_0.getStartMillis(), minute(HOUR_0, 10) + 1)),
          second.query.getIntervals()
      );
    }

    @Test
    public void testPriorityQueueNarrowsSpecificSegmentDescriptors()
    {
      // Wrap runners in SpecificSegmentQueryRunner, as data servers do, so narrowed intervals reach each segment as a
      // narrowed descriptor.
      final List<SegmentDescriptor> descriptors = makeDescriptors(List.of(HOUR_0, HOUR_1, HOUR_1));
      final List<RecordingRunner> recordingRunners = List.of(
          new RecordingRunner(HOUR_0, 50),
          new RecordingRunner(HOUR_1, 10, 40),
          new RecordingRunner(HOUR_1, 20, 50)
      );

      Assertions.assertEquals(
          List.of(minute(HOUR_1, 50), minute(HOUR_1, 40)),
          runMerged(
              Order.DESCENDING,
              2,
              List.of(HOUR_0, HOUR_1, HOUR_1),
              IntStream.range(0, descriptors.size())
                       .mapToObj(
                           i -> (QueryRunner<ScanResultValue>) new SpecificSegmentQueryRunner<>(
                               recordingRunners.get(i),
                               new SpecificSegmentSpec(descriptors.get(i))
                           )
                       )
                       .collect(Collectors.toList())
          )
      );

      Assertions.assertNull(recordingRunners.get(0).query);
      Assertions.assertEquals(
          new MultipleSpecificSegmentSpec(
              List.of(new SegmentDescriptor(Intervals.utc(minute(HOUR_1, 20), HOUR_1.getEndMillis()), "v0", 1))
          ),
          recordingRunners.get(1).query.getQuerySegmentSpec()
      );
      Assertions.assertEquals(
          new MultipleSpecificSegmentSpec(List.of(descriptors.get(2))),
          recordingRunners.get(2).query.getQuerySegmentSpec()
      );
    }

    @Test
    public void testRunnersThatDoNotMatchDescriptors()
    {
      // One fewer runner than descriptors, so runners cannot be matched to intervals.
      final List<QueryRunner<ScanResultValue>> runners = List.of(
          new RecordingRunner(HOUR_0, 10, 20, 30),
          new RecordingRunner(HOUR_2, 10, 20, 30)
      );
      final List<Interval> descriptorIntervals = List.of(HOUR_0, HOUR_1, HOUR_2);

      // Priority queue strategy.
      Assertions.assertThrows(
          DruidException.class,
          () -> runMerged(Order.DESCENDING, 2, descriptorIntervals, runners)
      );

      // N-way merge strategy.
      Assertions.assertThrows(
          DruidException.class,
          () -> runMerged(Order.DESCENDING, CONFIG.getMaxRowsQueuedForOrdering() + 1, descriptorIntervals, runners)
      );
    }

    @Test
    public void testPriorityQueueDescendingSinkQueryRunnersSkipsOlderIntervals()
    {
      final RecordingRunner hour0Hydrant0 = new RecordingRunner(HOUR_0, 10, 20);
      final RecordingRunner hour0Hydrant1 = new RecordingRunner(HOUR_0, 30);
      final RecordingRunner hour1Hydrant0 = new RecordingRunner(HOUR_1, 10, 30);
      final RecordingRunner hour1Hydrant1 = new RecordingRunner(HOUR_1, 20);

      Assertions.assertEquals(
          List.of(minute(HOUR_1, 30), minute(HOUR_1, 20)),
          runMerged(
              Order.DESCENDING,
              2,
              List.of(HOUR_0, HOUR_1),
              new SinkQueryRunners<>(
                  List.of(
                      Pair.of(HOUR_0, hour0Hydrant0),
                      Pair.of(HOUR_0, hour0Hydrant1),
                      Pair.of(HOUR_1, hour1Hydrant0),
                      Pair.of(HOUR_1, hour1Hydrant1)
                  )
              )
          )
      );
      Assertions.assertEquals(
          List.of(false, false, true, true),
          ranFlags(hour0Hydrant0, hour0Hydrant1, hour1Hydrant0, hour1Hydrant1)
      );
    }

    @Test
    public void testNWayMergeDescendingSinkQueryRunners()
    {
      Assertions.assertEquals(
          List.of(
              minute(HOUR_1, 30),
              minute(HOUR_1, 20),
              minute(HOUR_1, 10),
              minute(HOUR_0, 30),
              minute(HOUR_0, 20),
              minute(HOUR_0, 10)
          ),
          runMerged(
              Order.DESCENDING,
              CONFIG.getMaxRowsQueuedForOrdering() + 1,
              List.of(HOUR_0, HOUR_1),
              new SinkQueryRunners<>(
                  List.of(
                      Pair.of(HOUR_0, new RecordingRunner(HOUR_0, 10, 20)),
                      Pair.of(HOUR_0, new RecordingRunner(HOUR_0, 30)),
                      Pair.of(HOUR_1, new RecordingRunner(HOUR_1, 10, 30)),
                      Pair.of(HOUR_1, new RecordingRunner(HOUR_1, 20))
                  )
              )
          )
      );
    }

    private static List<Long> runMerged(
        final Order order,
        final long limit,
        final List<Interval> descriptorIntervals,
        final Iterable<QueryRunner<ScanResultValue>> runners
    )
    {
      return toTimestamps(runMergedRows(order, limit, descriptorIntervals, runners));
    }

    private static List<ScanResultValue> runMergedRows(
        final Order order,
        final long limit,
        final List<Interval> descriptorIntervals,
        final Iterable<QueryRunner<ScanResultValue>> runners
    )
    {
      final ScanQuery query =
          Druids.newScanQueryBuilder()
                .dataSource("foo")
                .intervals(new MultipleSpecificSegmentSpec(makeDescriptors(descriptorIntervals)))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                .order(order)
                .limit(limit)
                .build();

      return FACTORY.mergeRunners(Execs.directExecutor(), runners)
                    .run(QueryPlus.wrap(query), ResponseContext.createEmpty())
                    .toList()
                    .stream()
                    .flatMap(batch -> batch.toSingleEventScanResultValues().stream())
                    .collect(Collectors.toList());
    }

    private static List<SegmentDescriptor> makeDescriptors(final List<Interval> intervals)
    {
      return IntStream.range(0, intervals.size())
                      .mapToObj(i -> new SegmentDescriptor(intervals.get(i), "v0", i))
                      .collect(Collectors.toList());
    }

    private static List<Long> toTimestamps(final List<ScanResultValue> rows)
    {
      return rows.stream()
                 .map(row -> row.getFirstEventTimestamp(ScanQuery.ResultFormat.RESULT_FORMAT_LIST))
                 .collect(Collectors.toList());
    }

    private static List<Boolean> ranFlags(final RecordingRunner... runners)
    {
      return Arrays.stream(runners).map(runner -> runner.query != null).collect(Collectors.toList());
    }

    private static long minute(final Interval hour, final int minute)
    {
      return hour.getStart().plusMinutes(minute).getMillis();
    }

    /**
     * Runner that returns one single-row batch per timestamp within the query intervals, in the time order of the
     * query, as a cursor would. Records the query it ran with.
     */
    private static class RecordingRunner implements QueryRunner<ScanResultValue>
    {
      @Nullable
      private final String segmentId;
      private final List<Long> timestamps;

      /**
       * Query this runner ran with, or null if it did not run.
       */
      @Nullable
      private ScanQuery query;

      RecordingRunner(final Interval hour, final int... minutes)
      {
        this(null, hour, minutes);
      }

      /**
       * Creates a runner whose rows have {@code segmentId}, or the default of
       * {@link ScanQueryTestHelper#generateScanResultValue} if null.
       */
      RecordingRunner(@Nullable final String segmentId, final Interval hour, final int... minutes)
      {
        this.segmentId = segmentId;
        this.timestamps = Arrays.stream(minutes)
                                .mapToObj(minute -> hour.getStart().plusMinutes(minute).getMillis())
                                .collect(Collectors.toList());
      }

      @Override
      public Sequence<ScanResultValue> run(
          final QueryPlus<ScanResultValue> queryPlus,
          final ResponseContext responseContext
      )
      {
        query = (ScanQuery) queryPlus.getQuery();
        final List<Interval> queryIntervals = query.getIntervals();
        final List<Long> orderedTimestamps =
            timestamps.stream()
                      .filter(t -> queryIntervals.stream().anyMatch(interval -> interval.contains(t)))
                      .collect(Collectors.toList());
        if (Order.DESCENDING.equals(query.getTimeOrder())) {
          Collections.reverse(orderedTimestamps);
        }
        return Sequences.simple(
            orderedTimestamps.stream()
                             .map(t -> makeRow(t, query.getResultFormat()))
                             .collect(Collectors.toList())
        );
      }

      private ScanResultValue makeRow(final long timestamp, final ScanQuery.ResultFormat resultFormat)
      {
        final ScanResultValue row = ScanQueryTestHelper.generateScanResultValue(timestamp, resultFormat, 1);
        return segmentId == null ? row : new ScanResultValue(segmentId, row.getColumns(), row.getEvents());
      }
    }
  }
}
