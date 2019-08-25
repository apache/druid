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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.gridkit.lab.jvm.perfdata.JStatData;
import org.gridkit.lab.jvm.perfdata.JStatData.LongCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.StringCounter;
import org.gridkit.lab.jvm.perfdata.JStatData.TickCounter;

import javax.annotation.Nullable;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JvmMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(JvmMonitor.class);

  private final Map<String, String[]> dimensions;
  private final long pid;

  @VisibleForTesting
  @Nullable
  final GcCounters gcCounters;

  @Nullable
  private final AllocationMetricCollector collector;

  public JvmMonitor()
  {
    this(ImmutableMap.of());
  }

  public JvmMonitor(Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public JvmMonitor(Map<String, String[]> dimensions, String feed)
  {
    this(dimensions, feed, JvmPidDiscoverer.instance());
  }

  public JvmMonitor(Map<String, String[]> dimensions, String feed, PidDiscoverer pidDiscoverer)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
    this.pid = Preconditions.checkNotNull(pidDiscoverer).getPid();
    this.collector = AllocationMetricCollectors.getAllocationMetricCollector();
    this.gcCounters = tryCreateGcCounters();
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitJvmMemMetrics(emitter);
    emitDirectMemMetrics(emitter);
    emitGcMetrics(emitter);
    emitThreadAllocationMetrics(emitter);

    return true;
  }

  private void emitThreadAllocationMetrics(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    if (collector != null) {
      long delta = collector.calculateDelta();
      emitter.emit(builder.build("jvm/heapAlloc/bytes", delta));
    }
  }

  /**
   * These metrics are going to be replaced by new jvm/gc/mem/* metrics
   */
  @Deprecated
  private void emitJvmMemMetrics(ServiceEmitter emitter)
  {
    // I have no idea why, but jvm/mem is slightly more than the sum of jvm/pool. Let's just include
    // them both.
    final Map<String, MemoryUsage> usages = ImmutableMap.of(
        "heap", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage(),
        "nonheap", ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage()
    );
    for (Map.Entry<String, MemoryUsage> entry : usages.entrySet()) {
      final String kind = entry.getKey();
      final MemoryUsage usage = entry.getValue();
      final ServiceMetricEvent.Builder builder = builder().setDimension("memKind", kind);
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/mem/max", usage.getMax()));
      emitter.emit(builder.build("jvm/mem/committed", usage.getCommitted()));
      emitter.emit(builder.build("jvm/mem/used", usage.getUsed()));
      emitter.emit(builder.build("jvm/mem/init", usage.getInit()));
    }

    // jvm/pool
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      final String kind = pool.getType() == MemoryType.HEAP ? "heap" : "nonheap";
      final MemoryUsage usage = pool.getUsage();
      final ServiceMetricEvent.Builder builder = builder()
          .setDimension("poolKind", kind)
          .setDimension("poolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/pool/max", usage.getMax()));
      emitter.emit(builder.build("jvm/pool/committed", usage.getCommitted()));
      emitter.emit(builder.build("jvm/pool/used", usage.getUsed()));
      emitter.emit(builder.build("jvm/pool/init", usage.getInit()));
    }
  }

  private void emitDirectMemMetrics(ServiceEmitter emitter)
  {
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      final ServiceMetricEvent.Builder builder = builder().setDimension("bufferpoolName", pool.getName());
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.build("jvm/bufferpool/capacity", pool.getTotalCapacity()));
      emitter.emit(builder.build("jvm/bufferpool/used", pool.getMemoryUsed()));
      emitter.emit(builder.build("jvm/bufferpool/count", pool.getCount()));
    }
  }

  private void emitGcMetrics(ServiceEmitter emitter)
  {
    if (gcCounters != null) {
      gcCounters.emit(emitter, dimensions);
    }
  }

  @Nullable
  private GcCounters tryCreateGcCounters()
  {
    try {
      return new GcCounters();
    }
    catch (RuntimeException e) {
      // in JDK11 jdk.internal.perf.Perf is not accessible, unless
      // --add-exports java.base/jdk.internal.perf=ALL-UNNAMED is set
      log.warn("Cannot initialize GC counters. If running JDK11 and above,"
               + " add --add-exports java.base/jdk.internal.perf=ALL-UNNAMED"
               + " to the JVM arguments to enable GC counters.");
    }
    return null;
  }

  /**
   * The following GC-related code is partially based on
   * https://github.com/aragozin/jvm-tools/blob/e0e37692648951440aa1a4ea5046261cb360df70/
   * sjk-core/src/main/java/org/gridkit/jvmtool/PerfCounterGcCpuUsageMonitor.java
   */
  private class GcCounters
  {
    private final List<GcGeneration> generations = new ArrayList<>();

    GcCounters()
    {
      // connect to itself
      final JStatData jStatData = JStatData.connect(pid);
      final Map<String, JStatData.Counter<?>> jStatCounters = jStatData.getAllCounters();

      generations.add(new GcGeneration(jStatCounters, 0, "young"));
      generations.add(new GcGeneration(jStatCounters, 1, "old"));
      // Removed in Java 8 but still actual for previous Java versions
      if (jStatCounters.containsKey("sun.gc.generation.2.name")) {
        generations.add(new GcGeneration(jStatCounters, 2, "perm"));
      }
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      for (GcGeneration generation : generations) {
        generation.emit(emitter, dimensions);
      }
    }
  }

  private class GcGeneration
  {
    private final String name;
    private final GcGenerationCollector collector;
    private final List<GcGenerationSpace> spaces = new ArrayList<>();

    GcGeneration(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex, String name)
    {
      this.name = StringUtils.toLowerCase(name);

      long spacesCount = ((JStatData.LongCounter) jStatCounters.get(
          StringUtils.format("sun.gc.generation.%d.spaces", genIndex)
      )).getLong();
      for (long spaceIndex = 0; spaceIndex < spacesCount; spaceIndex++) {
        spaces.add(new GcGenerationSpace(jStatCounters, genIndex, spaceIndex));
      }

      if (jStatCounters.containsKey(StringUtils.format("sun.gc.collector.%d.name", genIndex))) {
        collector = new GcGenerationCollector(jStatCounters, genIndex);
      } else {
        collector = null;
      }
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      ImmutableMap.Builder<String, String[]> dimensionsCopyBuilder = ImmutableMap
          .<String, String[]>builder()
          .putAll(dimensions)
          .put("gcGen", new String[]{name});

      if (collector != null) {
        dimensionsCopyBuilder.put("gcName", new String[]{collector.name});
      }

      Map<String, String[]> dimensionsCopy = dimensionsCopyBuilder.build();

      if (collector != null) {
        collector.emit(emitter, dimensionsCopy);
      }

      for (GcGenerationSpace space : spaces) {
        space.emit(emitter, dimensionsCopy);
      }
    }
  }

  private class GcGenerationCollector
  {
    private final String name;
    private final LongCounter invocationsCounter;
    private final TickCounter cpuCounter;
    private long lastInvocations = 0;
    private long lastCpuNanos = 0;

    GcGenerationCollector(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex)
    {
      String collectorKeyPrefix = StringUtils.format("sun.gc.collector.%d", genIndex);

      String nameKey = StringUtils.format("%s.name", collectorKeyPrefix);
      StringCounter nameCounter = (StringCounter) jStatCounters.get(nameKey);
      name = getReadableName(nameCounter.getString());

      invocationsCounter = (LongCounter) jStatCounters.get(StringUtils.format("%s.invocations", collectorKeyPrefix));
      cpuCounter = (TickCounter) jStatCounters.get(StringUtils.format("%s.time", collectorKeyPrefix));
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      long newInvocations = invocationsCounter.getLong();
      emitter.emit(builder.build("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;

      long newCpuNanos = cpuCounter.getNanos();
      emitter.emit(builder.build("jvm/gc/cpu", newCpuNanos - lastCpuNanos));
      lastCpuNanos = newCpuNanos;
    }

    private String getReadableName(String name)
    {
      switch (name) {
        // Young gen
        case "Copy":
          return "serial";
        case "PSScavenge":
          return "parallel";
        case "PCopy":
          return "cms";
        case "G1 incremental collections":
          return "g1";

        // Old gen
        case "MCS":
          return "serial";
        case "PSParallelCompact":
          return "parallel";
        case "CMS":
          return "cms";
        case "G1 stop-the-world full collections":
          return "g1";

        default:
          return name;
      }
    }
  }

  private class GcGenerationSpace
  {
    private final String name;

    private final LongCounter maxCounter;
    private final LongCounter capacityCounter;
    private final LongCounter usedCounter;
    private final LongCounter initCounter;

    GcGenerationSpace(Map<String, JStatData.Counter<?>> jStatCounters, long genIndex, long spaceIndex)
    {
      String spaceKeyPrefix = StringUtils.format("sun.gc.generation.%d.space.%d", genIndex, spaceIndex);

      String nameKey = StringUtils.format("%s.name", spaceKeyPrefix);
      StringCounter nameCounter = (StringCounter) jStatCounters.get(nameKey);
      name = StringUtils.toLowerCase(nameCounter.toString());

      maxCounter = (LongCounter) jStatCounters.get(StringUtils.format("%s.maxCapacity", spaceKeyPrefix));
      capacityCounter = (LongCounter) jStatCounters.get(StringUtils.format("%s.capacity", spaceKeyPrefix));
      usedCounter = (LongCounter) jStatCounters.get(StringUtils.format("%s.used", spaceKeyPrefix));
      initCounter = (LongCounter) jStatCounters.get(StringUtils.format("%s.initCapacity", spaceKeyPrefix));
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      builder.setDimension("gcGenSpaceName", name);

      emitter.emit(builder.build("jvm/gc/mem/max", maxCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/capacity", capacityCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/used", usedCounter.getLong()));
      emitter.emit(builder.build("jvm/gc/mem/init", initCounter.getLong()));
    }
  }
}
