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

import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JvmMonitorTest
{

  private static final Set<String> VALID_PAUSE_TYPES = Set.of("stw", "concurrent");

  @Test(timeout = 60_000L)
  public void testGcCounts() throws InterruptedException
  {
    GcTrackingEmitter emitter = new GcTrackingEmitter();

    final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
    serviceEmitter.start();
    final JvmMonitor jvmMonitor = new JvmMonitor();
    // skip tests if gc counters fail to initialize with this JDK
    Assume.assumeNotNull(jvmMonitor.gcCollectors);

    while (true) {
      // generate some garbage to see gc counters incremented
      @SuppressWarnings("unused")
      byte[] b = new byte[1024 * 1024 * 50];
      emitter.reset();
      jvmMonitor.doMonitor(serviceEmitter);
      if (emitter.gcSeen()) {
        return;
      }
      Thread.sleep(10);
    }
  }

  @Test
  public void testGcNotificationMetricsEmitted()
  {
    EventCollectingEmitter emitter = new EventCollectingEmitter();
    final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
    serviceEmitter.start();

    final JvmMonitor jvmMonitor = new JvmMonitor();
    jvmMonitor.start();
    jvmMonitor.doMonitor(serviceEmitter);

    Set<String> metricNames = emitter.events.stream()
        .map(e -> ((ServiceMetricEvent) e).getMetric())
        .collect(Collectors.toSet());

    // Rate/gauge metrics should always be emitted (even if zero)
    Assert.assertTrue("Expected jvm/gc/allocationRate/bytes", metricNames.contains("jvm/gc/allocationRate/bytes"));
    Assert.assertTrue("Expected jvm/gc/promotionRate/bytes", metricNames.contains("jvm/gc/promotionRate/bytes"));
    Assert.assertTrue("Expected jvm/gc/liveDataSize/bytes", metricNames.contains("jvm/gc/liveDataSize/bytes"));
    Assert.assertTrue("Expected jvm/gc/maxDataSize/bytes", metricNames.contains("jvm/gc/maxDataSize/bytes"));

    // Verify jvmVersion dimension on the new metrics
    for (Event e : emitter.events) {
      ServiceMetricEvent event = (ServiceMetricEvent) e;
      String metric = event.getMetric();
      if (metric.startsWith("jvm/gc/allocationRate")
          || metric.startsWith("jvm/gc/promotionRate")
          || metric.startsWith("jvm/gc/liveDataSize")
          || metric.startsWith("jvm/gc/maxDataSize")) {
        Map<String, Object> map = event.toMap();
        Assert.assertNotNull("jvmVersion dimension should be set on " + metric, map.get("jvmVersion"));
      }
    }

    jvmMonitor.stop();
  }

  @Test(timeout = 60_000L)
  public void testPauseMetricsHaveDimensions() throws InterruptedException
  {
    EventCollectingEmitter emitter = new EventCollectingEmitter();
    final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
    serviceEmitter.start();

    final JvmMonitor jvmMonitor = new JvmMonitor();
    jvmMonitor.start();

    // Generate garbage to trigger GC events
    while (true) {
      @SuppressWarnings("unused")
      byte[] b = new byte[1024 * 1024 * 50];
      emitter.events.clear();
      jvmMonitor.doMonitor(serviceEmitter);

      boolean hasPauseOrConcurrent = emitter.events.stream()
          .map(e -> ((ServiceMetricEvent) e).getMetric())
          .anyMatch(m -> "jvm/gc/pause".equals(m) || "jvm/gc/concurrentPhaseTime".equals(m));

      if (hasPauseOrConcurrent) {
        // Verify dimensions on pause/concurrentPhaseTime events
        for (Event e : emitter.events) {
          ServiceMetricEvent event = (ServiceMetricEvent) e;
          String metric = event.getMetric();
          if ("jvm/gc/pause".equals(metric) || "jvm/gc/concurrentPhaseTime".equals(metric)) {
            Map<String, Object> map = event.toMap();
            Assert.assertNotNull("gcAction should be set on " + metric, map.get("gcAction"));
            Assert.assertNotNull("gcCause should be set on " + metric, map.get("gcCause"));
            Assert.assertNotNull("jvmVersion should be set on " + metric, map.get("jvmVersion"));
          }
        }
        jvmMonitor.stop();
        return;
      }
      Thread.sleep(10);
    }
  }

  private static class EventCollectingEmitter implements Emitter
  {
    final List<Event> events = new ArrayList<>();

    @Override
    public void start()
    {
    }

    @Override
    public void emit(Event event)
    {
      events.add(event);
    }

    @Override
    public void flush()
    {
    }

    @Override
    public void close()
    {
    }
  }

  private static class GcTrackingEmitter implements Emitter
  {
    private Number oldGcCount;
    private Number oldGcCpu;
    private Number youngGcCount;
    private Number youngGcCpu;

    @Override
    public void start()
    {

    }

    void reset()
    {
      oldGcCount = null;
      oldGcCpu = null;
      youngGcCount = null;
      youngGcCpu = null;
    }

    @Override
    public void emit(Event e)
    {
      ServiceMetricEvent event = (ServiceMetricEvent) e;
      String gcGen = null;
      if (event.toMap().get("gcGen") != null) {
        gcGen = ((List) event.toMap().get("gcGen")).get(0).toString();
      }

      if (event.toMap().get("gcPauseType") != null) {
        String pauseType = ((List) event.toMap().get("gcPauseType")).get(0).toString();
        Assert.assertTrue(
            "expected gcPauseType to be 'stw' or 'concurrent', got: " + pauseType,
            VALID_PAUSE_TYPES.contains(pauseType)
        );
      }

      switch (event.getMetric() + "/" + gcGen) {
        case "jvm/gc/count/old":
          oldGcCount = event.getValue();
          break;
        case "jvm/gc/cpu/old":
          oldGcCpu = event.getValue();
          break;
        case "jvm/gc/count/young":
          youngGcCount = event.getValue();
          break;
        case "jvm/gc/cpu/young":
          youngGcCpu = event.getValue();
          break;
      }
    }

    boolean gcSeen()
    {
      return oldGcSeen() || youngGcSeen();
    }

    private boolean oldGcSeen()
    {
      boolean oldGcCountSeen = oldGcCount != null && oldGcCount.longValue() > 0;
      boolean oldGcCpuSeen = oldGcCpu != null && oldGcCpu.longValue() > 0;
      if (oldGcCountSeen || oldGcCpuSeen) {
        System.out.println("old count: " + oldGcCount + ", cpu: " + oldGcCpu);
      }
      Assert.assertFalse(
          "expected to see old gc count and cpu both zero or non-existent or both positive",
          oldGcCountSeen ^ oldGcCpuSeen
      );
      return oldGcCountSeen;
    }

    private boolean youngGcSeen()
    {
      boolean youngGcCountSeen = youngGcCount != null && youngGcCount.longValue() > 0;
      boolean youngGcCpuSeen = youngGcCpu != null && youngGcCpu.longValue() > 0;
      if (youngGcCountSeen || youngGcCpuSeen) {
        System.out.println("young count: " + youngGcCount + ", cpu: " + youngGcCpu);
      }
      Assert.assertFalse(
          "expected to see young gc count and cpu both zero/non-existent or both positive",
          youngGcCountSeen ^ youngGcCpuSeen
      );
      return youngGcCountSeen;
    }

    @Override
    public void flush()
    {

    }

    @Override
    public void close()
    {

    }
  }
}
