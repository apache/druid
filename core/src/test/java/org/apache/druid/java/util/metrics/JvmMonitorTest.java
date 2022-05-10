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

import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

public class JvmMonitorTest
{

  @Test(timeout = 60_000L)
  public void testGcCounts() throws InterruptedException
  {
    GcTrackingEmitter emitter = new GcTrackingEmitter();

    final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "localhost", emitter);
    serviceEmitter.start();
    final JvmMonitor jvmMonitor = new JvmMonitor();
    // skip tests if gc counters fail to initialize with this JDK
    Assume.assumeNotNull(jvmMonitor.gcCounters);

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
  public void testGetGcGenerations()
  {
    Assert.assertEquals(
        IntSet.of(0, 1),
        JvmMonitor.getGcGenerations(
            ImmutableSet.of(
                "sun.gc.collector.0.name",
                "sun.gc.collector.1.name",
                "sun.gc.generation.1.spaces"
            )
        )
    );

    Assert.assertEquals(
        IntSet.of(1, 2),
        JvmMonitor.getGcGenerations(
            ImmutableSet.of(
                "sun.gc.generation.1.spaces",
                "sun.gc.collector.2.name",
                "sun.gc.somethingelse.3.name"
            )
        )
    );
  }

  @Test
  public void testGetGcGenerationName()
  {
    Assert.assertEquals("young", JvmMonitor.getGcGenerationName(0));
    Assert.assertEquals("old", JvmMonitor.getGcGenerationName(1));
    Assert.assertEquals("perm", JvmMonitor.getGcGenerationName(2));
    Assert.assertEquals("3", JvmMonitor.getGcGenerationName(3));
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
