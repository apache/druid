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

package org.apache.druid.segment.realtime;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;

public class RealtimeMetricsMonitorTest
{

  private StubServiceEmitter emitter;
  private Random random;

  @Before
  public void setup()
  {
    random = new Random(100);
    emitter = new StubServiceEmitter("test", "localhost");
  }

  @Test
  public void testDoMonitor()
  {
    FireDepartment fireDepartment = new FireDepartment(
        new DataSchema("wiki", null, null, null, null, null, null, new DefaultObjectMapper()),
        new RealtimeIOConfig(null, null),
        null
    );

    // Add some metrics and invoke monitoring
    final FireDepartmentMetrics metrics = fireDepartment.getMetrics();
    invokeRandomTimes(metrics::incrementThrownAway);
    invokeRandomTimes(metrics::incrementUnparseable);
    invokeRandomTimes(metrics::incrementProcessed);
    invokeRandomTimes(metrics::incrementDedup);
    invokeRandomTimes(metrics::incrementFailedHandoffs);
    invokeRandomTimes(metrics::incrementFailedPersists);
    invokeRandomTimes(metrics::incrementHandOffCount);
    invokeRandomTimes(metrics::incrementNumPersists);

    metrics.incrementPushedRows(random.nextInt());
    metrics.incrementRowOutputCount(random.nextInt());
    metrics.incrementMergedRows(random.nextInt());
    metrics.incrementMergeCpuTime(random.nextInt());
    metrics.setSinkCount(random.nextInt());

    RealtimeMetricsMonitor monitor = new RealtimeMetricsMonitor(Collections.singletonList(fireDepartment));
    monitor.doMonitor(emitter);

    // Verify the metrics
    emitter.verifyValue("ingest/events/thrownAway", metrics.thrownAway());
    emitter.verifyValue("ingest/events/unparseable", metrics.unparseable());

    emitter.verifyValue("ingest/events/duplicate", metrics.dedup());
    emitter.verifyValue("ingest/events/processed", metrics.processed());
    emitter.verifyValue("ingest/rows/output", metrics.rowOutput());
    emitter.verifyValue("ingest/persists/count", metrics.numPersists());
    emitter.verifyValue("ingest/persists/time", metrics.persistTimeMillis());
    emitter.verifyValue("ingest/persists/cpu", metrics.persistCpuTime());
    emitter.verifyValue("ingest/persists/backPressure", metrics.persistBackPressureMillis());
    emitter.verifyValue("ingest/persists/failed", metrics.failedPersists());
    emitter.verifyValue("ingest/handoff/failed", metrics.failedHandoffs());
    emitter.verifyValue("ingest/merge/time", metrics.mergeTimeMillis());
    emitter.verifyValue("ingest/merge/cpu", metrics.mergeCpuTime());
    emitter.verifyValue("ingest/handoff/count", metrics.handOffCount());
    emitter.verifyValue("ingest/sink/count", metrics.sinkCount());
  }

  private void invokeRandomTimes(Action action)
  {
    int limit = random.nextInt(20);
    for (int i = 0; i < limit; ++i) {
      action.perform();
    }
  }

  @FunctionalInterface
  private interface Action
  {
    void perform();
  }

}
