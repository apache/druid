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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


@RunWith(Parameterized.class)
public class ScanQueryRunnerFactoryTest
{
  private int numElements;
  private ScanQuery query;
  private ScanQuery.ResultFormat resultFormat;

  private static final ScanQueryRunnerFactory factory = new ScanQueryRunnerFactory(
      new ScanQueryQueryToolChest(
          new ScanQueryConfig(),
          DefaultGenericQueryMetricsFactory.instance()
      ),
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );

  public ScanQueryRunnerFactoryTest(
      final int numElements,
      final int batchSize,
      final long limit,
      final ScanQuery.ResultFormat resultFormat,
      final ScanQuery.TimeOrder timeOrder
  )
  {
    this.numElements = numElements;
    this.query = Druids.newScanQueryBuilder()
                       .batchSize(batchSize)
                       .limit(limit)
                       .timeOrder(timeOrder)
                       .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
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
    List<Long> limits = ImmutableList.of(3L, 1000L);
    List<ScanQuery.ResultFormat> resultFormats = ImmutableList.of(
        ScanQuery.ResultFormat.RESULT_FORMAT_LIST,
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST
    );
    List<ScanQuery.TimeOrder> timeOrder = ImmutableList.of(
        ScanQuery.TimeOrder.ASCENDING,
        ScanQuery.TimeOrder.DESCENDING
    );

    return QueryRunnerTestHelper.cartesian(
        numsElements,
        batchSizes,
        limits,
        resultFormats,
        timeOrder
    );
  }

  @Test
  public void testSortAndLimitScanResultValues()
  {
    List<ScanResultValue> srvs = new ArrayList<>(numElements);
    List<Long> expectedEventTimestamps = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      long timestamp = (ThreadLocalRandom.current().nextLong());
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
      if (query.getTimeOrder().equals(ScanQuery.TimeOrder.DESCENDING)) {
        return retVal * -1;
      }
      return retVal;
    });
    Sequence<ScanResultValue> inputSequence = Sequences.simple(srvs);
    List<ScanResultValue> output =
        factory.sortAndLimitScanResultValues(
            inputSequence,
            query
        ).toList();

    // check each scan result value has one event
    for (ScanResultValue srv : output) {
      if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)) {
        Assert.assertTrue(ScanQueryTestHelper.getEventsCompactedListResultFormat(srv).size() == 1);
      } else if (resultFormat.equals(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)) {
        Assert.assertTrue(ScanQueryTestHelper.getEventsListResultFormat(srv).size() == 1);
      }
    }

    // check total # of rows <= limit
    Assert.assertTrue(output.size() <= query.getLimit());

    // check ordering is correct
    for (int i = 1; i < output.size(); i++) {
      if (query.getTimeOrder().equals(ScanQuery.TimeOrder.DESCENDING)) {
        Assert.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) <
                          output.get(i - 1).getFirstEventTimestamp(resultFormat));
      } else {
        Assert.assertTrue(output.get(i).getFirstEventTimestamp(resultFormat) >
                          output.get(i - 1).getFirstEventTimestamp(resultFormat));
      }
    }

    // check the values are correct
    for (int i = 0; i < query.getLimit() && i < output.size(); i++) {
      Assert.assertEquals((long) expectedEventTimestamps.get(i), output.get(i).getFirstEventTimestamp(resultFormat));
    }
  }
}
