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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@ParameterizedClass
@MethodSource("constructorFeeder")
public class ScanQueryLimitRowIteratorTest extends InitializedNullHandlingTest
{
  private static final int NUM_ELEMENTS = 1000;
  private final int batchSize;
  private final int limit;
  private static List<ScanResultValue> singleEventScanResultValues = new ArrayList<>();
  private static List<ScanResultValue> multiEventScanResultValues = new ArrayList<>();
  private static final ScanQuery.ResultFormat RESULT_FORMAT = ScanQuery.ResultFormat.RESULT_FORMAT_LIST;

  public ScanQueryLimitRowIteratorTest(
      final int batchSize,
      final int limit
  )
  {
    this.batchSize = batchSize;
    this.limit = limit;
  }

  public static Iterable<Object[]> constructorFeeder()
  {
    List<Integer> batchSizes = ImmutableList.of(1, 33);
    List<Integer> limits = ImmutableList.of(3, 10000);
    return QueryRunnerTestHelper.cartesian(
        batchSizes,
        limits
    );
  }

  @BeforeEach
  public void setup()
  {
    singleEventScanResultValues = new ArrayList<>();
    multiEventScanResultValues = new ArrayList<>();
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      singleEventScanResultValues.add(
          ScanQueryTestHelper.generateScanResultValue(
              ThreadLocalRandom.current().nextLong(),
              RESULT_FORMAT,
              1
          ));
    }
    for (int i = 0; i < NUM_ELEMENTS / batchSize; i++) {
      multiEventScanResultValues.add(
          ScanQueryTestHelper.generateScanResultValue(
              ThreadLocalRandom.current().nextLong(),
              RESULT_FORMAT,
              batchSize
          ));
    }
    multiEventScanResultValues.add(
        ScanQueryTestHelper.generateScanResultValue(
            ThreadLocalRandom.current().nextLong(),
            RESULT_FORMAT,
            NUM_ELEMENTS % batchSize
        ));
  }

  /**
   * Expect no batching to occur and limit to be applied
   */
  @Test
  public void testNonOrderedScan()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .limit(limit)
                            .order(Order.NONE)
                            .dataSource("some datasource")
                            .batchSize(batchSize)
                            .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                            .resultFormat(RESULT_FORMAT)
                            .context(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false))
                            .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query);
    ScanQueryLimitRowIterator itr = new ScanQueryLimitRowIterator(
        ((queryInput, responseContext) -> Sequences.simple(multiEventScanResultValues)),
        queryPlus,
        ResponseContext.createEmpty()
    );

    int count = 0;
    int expectedNumRows = Math.min(limit, NUM_ELEMENTS);

    while (itr.hasNext()) {
      ScanResultValue curr = itr.next();
      List<Map<String, Object>> events = ScanQueryTestHelper.getEventsListResultFormat(curr);
      if (events.size() != batchSize) {
        if (expectedNumRows - count > batchSize) {
          Assertions.fail("Batch size is incorrect");
        } else {
          Assertions.assertEquals(expectedNumRows - count, events.size());
        }
      }
      count += events.size();
    }
    Assertions.assertEquals(expectedNumRows, count);
  }

  /**
   * Expect batching to occur and limit to be applied on the Broker.  Input from Historical
   * is a sequence of single-event ScanResultValues.
   */
  @Test
  public void testBrokerOrderedScan()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .limit(limit)
                            .order(Order.DESCENDING)
                            .dataSource("some datasource")
                            .batchSize(batchSize)
                            .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                            .resultFormat(RESULT_FORMAT)
                            .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query);
    ScanQueryLimitRowIterator itr = new ScanQueryLimitRowIterator(
        ((queryInput, responseContext) -> Sequences.simple(singleEventScanResultValues)),
        queryPlus,
        ResponseContext.createEmpty()
    );

    int count = 0;
    int expectedNumRows = Math.min(limit, NUM_ELEMENTS);
    while (itr.hasNext()) {
      ScanResultValue curr = itr.next();
      List<Map<String, Object>> events = ScanQueryTestHelper.getEventsListResultFormat(curr);
      if (events.size() != batchSize) {
        if (expectedNumRows - count >= batchSize) {
          Assertions.fail("Batch size is incorrect");
        } else {
          Assertions.assertEquals(expectedNumRows - count, events.size());
        }
      }
      count += events.size();
    }
    Assertions.assertEquals(expectedNumRows, count);
  }

  /**
   * Expect no batching to occur and limit to be applied.  Input is a sequence of sorted single-event ScanResultValues
   * (unbatching and sorting occurs in ScanQueryRunnerFactory#mergeRunners()).
   */
  @Test
  public void testHistoricalOrderedScan()
  {
    ScanQuery query = Druids.newScanQueryBuilder()
                            .limit(limit)
                            .order(Order.DESCENDING)
                            .dataSource("some datasource")
                            .batchSize(batchSize)
                            .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                            .resultFormat(RESULT_FORMAT)
                            .context(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false))
                            .build();

    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query);
    ScanQueryLimitRowIterator itr = new ScanQueryLimitRowIterator(
        ((queryInput, responseContext) -> Sequences.simple(singleEventScanResultValues)),
        queryPlus,
        ResponseContext.createEmpty()
    );

    int count = 0;
    int expectedNumRows = Math.min(limit, NUM_ELEMENTS);
    while (itr.hasNext()) {
      ScanResultValue curr = itr.next();
      List<Map<String, Object>> events = ScanQueryTestHelper.getEventsListResultFormat(curr);
      Assertions.assertEquals(1, events.size());
      count += events.size();
    }
    Assertions.assertEquals(expectedNumRows, count);
  }
}
