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

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricQueryRunnerTest
{
  @Test
  public void testCpuTimeMetric()
  {
    final StubServiceEmitter emitter = new StubServiceEmitter("s", "h");
    final AtomicLong accumulator = new AtomicLong();

    final List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2000-01-01"),
            new TimeseriesResultValue(ImmutableMap.of("x", "y"))
        )
    );

    final QueryRunner<Result<TimeseriesResultValue>> runner = CPUTimeMetricQueryRunner.safeBuild(
        (queryPlus, responseContext) -> Sequences.simple(expectedResults),
        new TimeseriesQueryQueryToolChest(),
        emitter,
        accumulator,
        true
    );

    final Sequence<Result<TimeseriesResultValue>> results = runner.run(
        QueryPlus.wrap(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals("2000/2001")
                  .build()
        ).withQueryMetrics(new TimeseriesQueryQueryToolChest())
    );

    Assert.assertEquals(expectedResults, results.toList());
    Assert.assertEquals(1, emitter.getEvents().size());

    final Event event = Iterables.getOnlyElement(emitter.getEvents());

    Assert.assertEquals("metrics", event.toMap().get("feed"));
    Assert.assertEquals("query/cpu/time", event.toMap().get("metric"));

    final Object value = event.toMap().get("value");
    Assert.assertThat(value, CoreMatchers.instanceOf(Long.class));
    Assert.assertTrue((long) value > 0);
  }
}
