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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@RunWith(Enclosed.class)
public class ScanQueryRunnerFactoryTest
{

  private static final ScanQueryRunnerFactory FACTORY = new ScanQueryRunnerFactory(
      new ScanQueryQueryToolChest(
          new ScanQueryConfig(),
          DefaultGenericQueryMetricsFactory.instance()
      ),
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );

  @RunWith(Parameterized.class)
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
        final ScanQuery.Order order
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

    @Parameterized.Parameters(name = "{0} {1} {2} {3} {4}")
    public static Iterable<Object[]> constructorFeeder()
    {
      List<Integer> numsElements = ImmutableList.of(0, 10, 100);
      List<Integer> batchSizes = ImmutableList.of(1, 100);
      List<Long> limits = ImmutableList.of(3L, 1000L, Long.MAX_VALUE);
      List<ScanQuery.ResultFormat> resultFormats = ImmutableList.of(
          ScanQuery.ResultFormat.RESULT_FORMAT_LIST,
          ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST
      );
      List<ScanQuery.Order> order = ImmutableList.of(
          ScanQuery.Order.ASCENDING,
          ScanQuery.Order.DESCENDING
      );

      return QueryRunnerTestHelper.cartesian(
          numsElements,
          batchSizes,
          limits,
          resultFormats,
          order
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
        if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
          return retVal * -1;
        }
        return retVal;
      });
      Sequence<ScanResultValue> inputSequence = Sequences.simple(srvs);
      try {
        List<ScanResultValue> output = FACTORY.priorityQueueSortAndLimit(
            inputSequence,
            query,
            ImmutableList.of(new Interval(
                DateTimes.of("2010-01-01"),
                DateTimes.of("2019-01-01").plusHours(1)
            ))
        ).toList();
        if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
          Assert.fail("Unsupported exception should have been thrown due to high limit");
        }
        validateSortedOutput(output, expectedEventTimestamps);
      }
      catch (UOE e) {
        if (query.getScanRowsLimit() <= Integer.MAX_VALUE) {
          Assert.fail("Unsupported operation exception should not have been thrown here");
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

      if (query.getOrder() == ScanQuery.Order.DESCENDING) {
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

      if (query.getOrder() == ScanQuery.Order.DESCENDING) {
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
        if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
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
          Assert.assertTrue(ScanQueryTestHelper.getEventsCompactedListResultFormat(srv).size() == 1);
        } else if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)) {
          Assert.assertTrue(ScanQueryTestHelper.getEventsListResultFormat(srv).size() == 1);
        }
      }

      // check total # of rows <= limit
      Assert.assertTrue(output.size() <= query.getScanRowsLimit());

      // check ordering is correct
      for (int i = 1; i < output.size(); i++) {
        if (query.getOrder().equals(ScanQuery.Order.DESCENDING)) {
          Assert.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) <
                            output.get(i - 1).getFirstEventTimestamp(resultFormat));
        } else {
          Assert.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) >
                            output.get(i - 1).getFirstEventTimestamp(resultFormat));
        }
      }

      // check the values are correct
      for (int i = 0; i < query.getScanRowsLimit() && i < output.size(); i++) {
        Assert.assertEquals((long) expectedEventTimestamps.get(i), output.get(i).getFirstEventTimestamp(resultFormat));
      }
    }
  }

  public static class ScanQueryRunnerFactoryNonParameterizedTest
  {
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
      Assert.assertEquals(1, intervals.size());
      Assert.assertEquals(descriptor.getInterval(), intervals.get(0));

      intervals = FACTORY.getIntervalsFromSpecificQuerySpec(singleSpecificSpec);
      Assert.assertEquals(1, intervals.size());
      Assert.assertEquals(descriptor.getInterval(), intervals.get(0));
    }

    @Test(expected = UOE.class)
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
      FACTORY.getIntervalsFromSpecificQuerySpec(multiIntervalSpec);
    }

    @Test(expected = UOE.class)
    public void testGetSegmentDescriptorsFromInvalidLegacySpec()
    {
      QuerySegmentSpec legacySpec = new LegacySegmentSpec(
          new Interval(
              DateTimes.of("2010-01-01"),
              DateTimes.of("2019-01-01").plusHours(1)
          )
      );
      FACTORY.getIntervalsFromSpecificQuerySpec(legacySpec);
    }
  }
}
