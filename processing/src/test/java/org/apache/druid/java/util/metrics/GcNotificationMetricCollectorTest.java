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

package org.apache.druid.java.util.metrics;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.metrics.GcNotificationMetricCollector.DrainResult;
import org.apache.druid.java.util.metrics.GcNotificationMetricCollector.GcPauseEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.management.MemoryUsage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class GcNotificationMetricCollectorTest
{
  private GcNotificationMetricCollector collector;

  @Before
  public void setUp()
  {
    collector = new GcNotificationMetricCollector(Collections.emptyList());
  }

  // Pool classification tests

  @Test
  public void testIsYoungGenPool()
  {
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("PS Eden Space"));
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("G1 Eden Space"));
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("Par Eden Space"));
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("Copy Young Gen"));
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("Shenandoah"));
    Assert.assertTrue(GcNotificationMetricCollector.isYoungGenPool("ZHeap"));

    Assert.assertFalse(GcNotificationMetricCollector.isYoungGenPool("PS Old Gen"));
    Assert.assertFalse(GcNotificationMetricCollector.isYoungGenPool("G1 Survivor Space"));
    Assert.assertFalse(GcNotificationMetricCollector.isYoungGenPool("Metaspace"));
  }

  @Test
  public void testIsOldGenPool()
  {
    Assert.assertTrue(GcNotificationMetricCollector.isOldGenPool("PS Old Gen"));
    Assert.assertTrue(GcNotificationMetricCollector.isOldGenPool("G1 Old Gen"));
    Assert.assertTrue(GcNotificationMetricCollector.isOldGenPool("Tenured Gen"));
    Assert.assertTrue(GcNotificationMetricCollector.isOldGenPool("Shenandoah"));
    Assert.assertTrue(GcNotificationMetricCollector.isOldGenPool("ZHeap"));

    Assert.assertFalse(GcNotificationMetricCollector.isOldGenPool("PS Eden Space"));
    Assert.assertFalse(GcNotificationMetricCollector.isOldGenPool("G1 Survivor Space"));
    Assert.assertFalse(GcNotificationMetricCollector.isOldGenPool("Metaspace"));
  }

  // Concurrent phase detection tests

  @Test
  public void testIsConcurrentPhase()
  {
    Assert.assertTrue(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("G1 Concurrent GC", "No GC", "end of minor GC")
    ));
    Assert.assertTrue(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("ZGC Cycles", "Proactive", "end of major GC")
    ));
    Assert.assertTrue(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("Shenandoah Cycles", "Allocation Failure", "end of minor GC")
    ));
    Assert.assertTrue(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("G1 Young Generation", "No GC", "end of minor GC")
    ));

    Assert.assertFalse(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("G1 Young Generation", "Allocation Failure", "end of minor GC")
    ));
    Assert.assertFalse(GcNotificationMetricCollector.isConcurrentPhase(
        mockNotificationInfo("PS MarkSweep", "Ergonomics", "end of major GC")
    ));
  }

  // Simulated notification handling tests

  @Test
  public void testAllocationRateCalculation()
  {
    // First GC: eden 100MB before, 0 after
    GarbageCollectionNotificationInfo info1 = createYoungGcInfo(
        100 * 1024 * 1024L, // eden before
        0,                   // eden after
        50 * 1024 * 1024L,   // old before
        50 * 1024 * 1024L,   // old after
        200 * 1024 * 1024L,  // old max
        50                    // duration ms
    );
    collector.processGcEvent(info1);

    // No allocation rate on first GC (no previous young gen size)
    DrainResult result1 = collector.drain();
    Assert.assertEquals(0, result1.getAllocationRateBytes());

    // Second GC: eden 80MB before (i.e. 80MB allocated since last GC left eden at 0), 0 after
    GarbageCollectionNotificationInfo info2 = createYoungGcInfo(
        80 * 1024 * 1024L,
        0,
        50 * 1024 * 1024L,
        52 * 1024 * 1024L,
        200 * 1024 * 1024L,
        30
    );
    collector.processGcEvent(info2);

    DrainResult result2 = collector.drain();
    Assert.assertEquals(80 * 1024 * 1024L, result2.getAllocationRateBytes());
  }

  @Test
  public void testPromotionRate()
  {
    GarbageCollectionNotificationInfo info = createYoungGcInfo(
        100 * 1024 * 1024L,
        0,
        50 * 1024 * 1024L,  // old before
        60 * 1024 * 1024L,  // old after (10MB promoted)
        200 * 1024 * 1024L,
        50
    );
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    Assert.assertEquals(10 * 1024 * 1024L, result.getPromotionRateBytes());
  }

  @Test
  public void testNoPromotionWhenOldGenShrinks()
  {
    GarbageCollectionNotificationInfo info = createYoungGcInfo(
        100 * 1024 * 1024L,
        0,
        60 * 1024 * 1024L,  // old before
        50 * 1024 * 1024L,  // old after (shrunk, no promotion)
        200 * 1024 * 1024L,
        50
    );
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    Assert.assertEquals(0, result.getPromotionRateBytes());
  }

  @Test
  public void testLiveAndMaxDataSizeUpdatedAfterMajorGc()
  {
    GarbageCollectionNotificationInfo info = createOldGcInfo(
        0,
        0,
        100 * 1024 * 1024L,
        40 * 1024 * 1024L,  // old after = live data size
        512 * 1024 * 1024L, // old max = max data size
        200
    );
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    Assert.assertEquals(40 * 1024 * 1024L, result.getLiveDataSizeBytes());
    Assert.assertEquals(512 * 1024 * 1024L, result.getMaxDataSizeBytes());
  }

  @Test
  public void testLiveDataSizeNotUpdatedAfterYoungGc()
  {
    GarbageCollectionNotificationInfo info = createYoungGcInfo(
        100 * 1024 * 1024L,
        0,
        50 * 1024 * 1024L,
        55 * 1024 * 1024L,
        200 * 1024 * 1024L,
        30
    );
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    // live data size not set by young GC
    Assert.assertEquals(0, result.getLiveDataSizeBytes());
  }

  @Test
  public void testPauseEventsCollected()
  {
    GarbageCollectionNotificationInfo info = createYoungGcInfo(
        100 * 1024 * 1024L,
        0,
        50 * 1024 * 1024L,
        50 * 1024 * 1024L,
        200 * 1024 * 1024L,
        42
    );
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    Assert.assertEquals(1, result.getPauseEvents().size());
    Assert.assertEquals(0, result.getConcurrentPhaseEvents().size());

    GcPauseEvent event = result.getPauseEvents().get(0);
    Assert.assertEquals(0.042, event.getDurationSeconds(), 0.0001);
    Assert.assertEquals("end of minor GC", event.getGcAction());
    Assert.assertEquals("Allocation Failure", event.getGcCause());
  }

  @Test
  public void testConcurrentPhaseEventsCollected()
  {
    GarbageCollectionNotificationInfo info = createConcurrentGcInfo(150);
    collector.processGcEvent(info);

    DrainResult result = collector.drain();
    Assert.assertEquals(0, result.getPauseEvents().size());
    Assert.assertEquals(1, result.getConcurrentPhaseEvents().size());

    GcPauseEvent event = result.getConcurrentPhaseEvents().get(0);
    Assert.assertEquals(0.15, event.getDurationSeconds(), 0.0001);
    Assert.assertEquals("end of minor GC", event.getGcAction());
    Assert.assertEquals("No GC", event.getGcCause());
  }

  // Drain behavior tests

  @Test
  public void testDrainResetsCountersButNotGauges()
  {
    // Major GC to set both counters and gauges
    GarbageCollectionNotificationInfo info1 = createOldGcInfo(
        50 * 1024 * 1024L,
        0,
        80 * 1024 * 1024L,
        30 * 1024 * 1024L,
        512 * 1024 * 1024L,
        100
    );
    collector.processGcEvent(info1);

    // First drain: should have promotion and live data
    DrainResult result1 = collector.drain();
    Assert.assertTrue(result1.getLiveDataSizeBytes() > 0);
    Assert.assertTrue(result1.getMaxDataSizeBytes() > 0);

    // Second drain: counters should be zero, gauges should persist
    DrainResult result2 = collector.drain();
    Assert.assertEquals(0, result2.getAllocationRateBytes());
    Assert.assertEquals(0, result2.getPromotionRateBytes());
    Assert.assertEquals(result1.getLiveDataSizeBytes(), result2.getLiveDataSizeBytes());
    Assert.assertEquals(result1.getMaxDataSizeBytes(), result2.getMaxDataSizeBytes());
    Assert.assertTrue(result2.getPauseEvents().isEmpty());
    Assert.assertTrue(result2.getConcurrentPhaseEvents().isEmpty());
  }

  // Thread safety test

  @Test(timeout = 30_000L)
  public void testConcurrentNotificationsAndDrain() throws InterruptedException
  {
    int numThreads = 4;
    int eventsPerThread = 1000;
    ExecutorService executor = Execs.multiThreaded(numThreads, "gc-test-%d");
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);

    for (int t = 0; t < numThreads; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < eventsPerThread; i++) {
            GarbageCollectionNotificationInfo info = createYoungGcInfo(
                1024L,
                0,
                100L,
                110L,
                1024L,
                1
            );
            collector.processGcEvent(info);
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    doneLatch.await(10, TimeUnit.SECONDS);
    executor.shutdown();

    // Drain all accumulated events
    DrainResult result = collector.drain();

    // All events should be accounted for (each event promotes 10 bytes)
    Assert.assertEquals((long) numThreads * eventsPerThread * 10L, result.getPromotionRateBytes());
    Assert.assertEquals(numThreads * eventsPerThread, result.getPauseEvents().size());
  }

  // Helper methods to create mock GC notification info

  private static GarbageCollectionNotificationInfo mockNotificationInfo(
      String gcName,
      String gcCause,
      String gcAction
  )
  {
    GarbageCollectionNotificationInfo info = Mockito.mock(GarbageCollectionNotificationInfo.class);
    Mockito.when(info.getGcName()).thenReturn(gcName);
    Mockito.when(info.getGcCause()).thenReturn(gcCause);
    Mockito.when(info.getGcAction()).thenReturn(gcAction);
    return info;
  }

  private static GarbageCollectionNotificationInfo createYoungGcInfo(
      long edenBefore,
      long edenAfter,
      long oldBefore,
      long oldAfter,
      long oldMax,
      long durationMs
  )
  {
    return createGcInfo(
        "G1 Young Generation",
        "Allocation Failure",
        "end of minor GC",
        edenBefore,
        edenAfter,
        oldBefore,
        oldAfter,
        oldMax,
        durationMs
    );
  }

  private static GarbageCollectionNotificationInfo createOldGcInfo(
      long edenBefore,
      long edenAfter,
      long oldBefore,
      long oldAfter,
      long oldMax,
      long durationMs
  )
  {
    return createGcInfo(
        "G1 Old Generation",
        "Ergonomics",
        "end of major GC",
        edenBefore,
        edenAfter,
        oldBefore,
        oldAfter,
        oldMax,
        durationMs
    );
  }

  private static GarbageCollectionNotificationInfo createConcurrentGcInfo(long durationMs)
  {
    return createGcInfo(
        "G1 Concurrent GC",
        "No GC",
        "end of minor GC",
        0, 0,
        50 * 1024 * 1024L,
        50 * 1024 * 1024L,
        200 * 1024 * 1024L,
        durationMs
    );
  }

  private static GarbageCollectionNotificationInfo createGcInfo(
      String gcName,
      String gcCause,
      String gcAction,
      long edenBefore,
      long edenAfter,
      long oldBefore,
      long oldAfter,
      long oldMax,
      long durationMs
  )
  {
    Map<String, MemoryUsage> beforeMap = new HashMap<>();
    beforeMap.put("G1 Eden Space", new MemoryUsage(0, edenBefore, edenBefore, edenBefore));
    beforeMap.put("G1 Old Gen", new MemoryUsage(0, oldBefore, oldBefore, oldMax));

    Map<String, MemoryUsage> afterMap = new HashMap<>();
    afterMap.put("G1 Eden Space", new MemoryUsage(0, edenAfter, edenAfter, edenAfter));
    afterMap.put("G1 Old Gen", new MemoryUsage(0, oldAfter, oldAfter, oldMax));

    GcInfo gcInfo = Mockito.mock(GcInfo.class);
    Mockito.when(gcInfo.getMemoryUsageBeforeGc()).thenReturn(beforeMap);
    Mockito.when(gcInfo.getMemoryUsageAfterGc()).thenReturn(afterMap);
    Mockito.when(gcInfo.getDuration()).thenReturn(durationMs);

    GarbageCollectionNotificationInfo info = Mockito.mock(GarbageCollectionNotificationInfo.class);
    Mockito.when(info.getGcName()).thenReturn(gcName);
    Mockito.when(info.getGcCause()).thenReturn(gcCause);
    Mockito.when(info.getGcAction()).thenReturn(gcAction);
    Mockito.when(info.getGcInfo()).thenReturn(gcInfo);

    return info;
  }
}
