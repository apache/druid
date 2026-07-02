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

package org.apache.druid.query;

import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ChainedExecutionQueryRunnerExtrapolationTest
{
  private QueryProcessingPool processingPool;

  @Before
  public void setup()
  {
    processingPool = new ForwardingQueryProcessingPool(
        Execs.multiThreaded(2, "ExtrapolationTestExecutor-%d"),
        Execs.scheduledSingleThreaded("ExtrapolationTestExecutor-Timeout-%d")
    );
  }

  @After
  public void tearDown()
  {
    processingPool.shutdown();
  }

  @Test(timeout = 10_000L)
  public void testExtrapolation_disabled_whenSamplingWindowZero()
  {
    QueryRunner slowRunner = sleepRunner(200);
    QueryRunner fastRunner = (queryPlus, responseContext) -> Sequences.of(1);

    ChainedExecutionQueryRunner runner = makeRunner(slowRunner, fastRunner);
    TimeseriesQuery query = makeQuery(Map.of(
        QueryContexts.TIMEOUT_KEY, 10_000L
    ));

    List results = runner.run(QueryPlus.wrap(query)).toList();
    Assert.assertNotNull(results);
    Assert.assertEquals(2, results.size());
  }

  @Test(timeout = 10_000L)
  public void testExtrapolation_cancelsQuery_whenProjectedTimeExceedsTimeout()
  {
    // 5 slow runners (300ms each on 2 threads), sampling window=2, timeout=500ms
    // After 2 complete: elapsed ~300ms, extrapolated wall-clock = 300 * 5/2 = 750ms > 500ms → cancel
    QueryRunner slowRunner = sleepRunner(300);

    ChainedExecutionQueryRunner runner = makeRunner(
        slowRunner,
        slowRunner,
        slowRunner,
        slowRunner,
        slowRunner
    );
    TimeseriesQuery query = makeQuery(Map.of(
        QueryContexts.TIMEOUT_KEY, 500L,
        QueryContexts.PER_SEGMENT_SAMPLING_WINDOW_KEY, 2
    ));

    Exception thrown = null;
    try {
      runner.run(QueryPlus.wrap(query)).toList();
    }
    catch (Exception e) {
      thrown = e;
    }

    Assert.assertNotNull("Expected exception from extrapolation", thrown);
    Assert.assertTrue(
        "Should be QueryTimeoutException",
        Throwables.getRootCause(thrown) instanceof QueryTimeoutException
    );
    Assert.assertTrue(
        "Message should mention extrapolation",
        thrown.getMessage().contains("extrapolated")
    );
  }

  @Test(timeout = 10_000L)
  public void testExtrapolation_doesNotCancel_whenProjectedTimeWithinTimeout()
  {
    QueryRunner fastRunner = (queryPlus, responseContext) -> Sequences.of(1);

    ChainedExecutionQueryRunner runner = makeRunner(
        fastRunner,
        fastRunner,
        fastRunner
    );
    TimeseriesQuery query = makeQuery(Map.of(
        QueryContexts.TIMEOUT_KEY, 30_000L,
        QueryContexts.PER_SEGMENT_SAMPLING_WINDOW_KEY, 2
    ));

    List results = runner.run(QueryPlus.wrap(query)).toList();
    Assert.assertNotNull(results);
    Assert.assertEquals(3, results.size());
  }

  @Test(timeout = 10_000L)
  public void testExtrapolation_skipped_whenFewerSegmentsThanSamplingWindow()
  {
    QueryRunner slowRunner = sleepRunner(200);

    ChainedExecutionQueryRunner runner = makeRunner(slowRunner, slowRunner);
    TimeseriesQuery query = makeQuery(Map.of(
        QueryContexts.TIMEOUT_KEY, 10_000L,
        QueryContexts.PER_SEGMENT_SAMPLING_WINDOW_KEY, 5
    ));

    List results = runner.run(QueryPlus.wrap(query)).toList();
    Assert.assertNotNull(results);
    Assert.assertEquals(2, results.size());
  }

  private QueryRunner sleepRunner(long sleepMs)
  {
    return (queryPlus, responseContext) -> {
      try {
        Thread.sleep(sleepMs);
        return Sequences.of(1);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private ChainedExecutionQueryRunner makeRunner(QueryRunner... runners)
  {
    QueryWatcher watcher = EasyMock.createNiceMock(QueryWatcher.class);
    EasyMock.replay(watcher);
    return new ChainedExecutionQueryRunner<>(
        processingPool,
        watcher,
        Arrays.asList(runners)
    );
  }

  private TimeseriesQuery makeQuery(Map<String, Object> context)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test")
                 .intervals("2014/2015")
                 .aggregators(List.of(new CountAggregatorFactory("count")))
                 .context(context)
                 .queryId("test")
                 .build();
  }
}
