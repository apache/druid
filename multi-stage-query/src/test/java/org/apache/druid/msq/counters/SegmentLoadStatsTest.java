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

package org.apache.druid.msq.counters;

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SegmentLoadStatsTest
{
  @Test
  public void test_report_fromChannelCounters()
  {
    final ChannelCounters counters1 = new ChannelCounters();
    counters1.addLoad(new AcquireSegmentResult(Optional.empty(), 100, millisToNanos(10), millisToNanos(30)));
    counters1.addLoad(new AcquireSegmentResult(Optional.empty(), 200, millisToNanos(40), millisToNanos(20)));

    // Not a load, since no bytes were loaded.
    counters1.addLoad(AcquireSegmentResult.empty());

    final ChannelCounters counters2 = new ChannelCounters();
    counters2.addLoad(new AcquireSegmentResult(Optional.empty(), 300, millisToNanos(1), millisToNanos(70)));

    final Map<String, Number> metrics = report(counters1.getLoadStats().plus(counters2.getLoadStats()));
    Assertions.assertEquals(
        Map.of(
            DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_COUNT, 3L,
            DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_BYTES, 600L,
            DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_AVG, 40L,
            DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_MAX, 70L,
            DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_AVG, 17L,
            DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_MAX, 40L
        ),
        metrics
    );
  }

  @Test
  public void test_report_empty()
  {
    Assertions.assertEquals(Map.of(), report(SegmentLoadStats.EMPTY));
    Assertions.assertEquals(Map.of(), report(new ChannelCounters().getLoadStats()));
  }

  private static Map<String, Number> report(final SegmentLoadStats stats)
  {
    final DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>();
    stats.report(queryMetrics);

    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    queryMetrics.emit(emitter);

    final Map<String, Number> retVal = new HashMap<>();
    for (final String metric : new String[]{
        DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_COUNT,
        DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_BYTES,
        DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_AVG,
        DefaultQueryMetrics.QUERY_ON_DEMAND_LOAD_TIME_MAX,
        DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_AVG,
        DefaultQueryMetrics.QUERY_ON_DEMAND_WAIT_TIME_MAX
    }) {
      for (final Number value : emitter.getMetricValues(metric, null)) {
        retVal.put(metric, value);
      }
    }
    return retVal;
  }

  private static long millisToNanos(final long millis)
  {
    return TimeUnit.MILLISECONDS.toNanos(millis);
  }
}
