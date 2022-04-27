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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
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
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
    this.collector = AllocationMetricCollectors.getAllocationMetricCollector();
    this.gcCounters = new GcCounters();
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

  /**
   * The following GC-related code is partially based on
   * https://github.com/aragozin/jvm-tools/blob/e0e37692648951440aa1a4ea5046261cb360df70/
   * sjk-core/src/main/java/org/gridkit/jvmtool/PerfCounterGcCpuUsageMonitor.java
   */
  private class GcCounters
  {
    private final List<GcCollectors> generations = new ArrayList<>();

    GcCounters()
    {
      List<GarbageCollectorMXBean> collectorMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean collectorMxBean : collectorMxBeans) {
        generations.add(new GcCollectors(collectorMxBean));
      }

    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      for (GcCollectors generation : generations) {
        generation.emit(emitter, dimensions);
      }
    }
  }

  private class GcCollectors
  {
    private final String generation;
    private final String collectorName;
    private final GcGenerationCollector collector;
    private final List<GcGenerationSpace> spaces = new ArrayList<>();

    private static final String GC_YOUNG_GENERATION_NAME = "young";
    private static final String GC_OLD_GENERATION_NAME = "old";
    private static final String GC_ZGC_GENERATION_NAME = "zgc";

    private static final String GC_CMS_NAME = "cms";
    private static final String GC_G1_NAME = "g1";
    private static final String GC_PARALLEL_NAME = "parallel";
    private static final String GC_SERIAL_NAME = "serial";
    private static final String GC_ZGC_NAME = "zgc";
    private static final String GC_SHENANDOAN_NAME = "shenandoah";

    GcCollectors(GarbageCollectorMXBean gcBean)
    {
      Pair<String, String> gcNamePair = getReadableName(gcBean.getName());
      this.generation = gcNamePair.lhs;
      this.collectorName = gcNamePair.rhs;

      collector = new GcGenerationCollector(gcBean);

      List<MemoryPoolMXBean> memoryPoolMxBeans = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean memoryPoolMxBean : memoryPoolMxBeans) {
        MemoryUsage collectionUsage = memoryPoolMxBean.getCollectionUsage();
        if (collectionUsage != null) {
          spaces.add(new GcGenerationSpace(collectionUsage, memoryPoolMxBean.getName()));
        }
      }
    }

    private Pair<String, String> getReadableName(String name)
    {
      switch (name) {
        //CMS
        case "ParNew":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, GC_CMS_NAME);
        case "ConcurrentMarkSweep":
          return new Pair<>(GC_OLD_GENERATION_NAME, GC_CMS_NAME);

        // G1
        case "G1 Young Generation":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, GC_G1_NAME);
        case "G1 Old Generation":
          return new Pair<>(GC_OLD_GENERATION_NAME, GC_G1_NAME);

        // Parallel
        case "PS Scavenge":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, GC_PARALLEL_NAME);
        case "PS MarkSweep":
          return new Pair<>(GC_OLD_GENERATION_NAME, GC_PARALLEL_NAME);

        // Serial
        case "Copy":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, GC_SERIAL_NAME);
        case "MarkSweepCompact":
          return new Pair<>(GC_OLD_GENERATION_NAME, GC_SERIAL_NAME);

        //zgc
        case "ZGC":
          return new Pair<>(GC_ZGC_GENERATION_NAME, GC_ZGC_NAME);

        //Shenandoah
        case "Shenandoah Cycles":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, GC_SHENANDOAN_NAME);
        case "Shenandoah Pauses":
          return new Pair<>(GC_OLD_GENERATION_NAME, GC_SHENANDOAN_NAME);

        default:
          return new Pair<>(name, name);
      }
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      ImmutableMap.Builder<String, String[]> dimensionsCopyBuilder = ImmutableMap
              .<String, String[]>builder()
              .putAll(dimensions)
              .put("gcGen", new String[]{generation});

      dimensionsCopyBuilder.put("gcName", new String[]{collectorName});

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
    private long lastInvocations = 0;
    private long lastCpuNanos = 0;
    private final GarbageCollectorMXBean gcBean;

    GcGenerationCollector(GarbageCollectorMXBean gcBean)
    {
      this.gcBean = gcBean;
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      long newInvocations = gcBean.getCollectionCount();
      emitter.emit(builder.build("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;

      long newCpuNanos = gcBean.getCollectionTime();
      emitter.emit(builder.build("jvm/gc/cpu", newCpuNanos - lastCpuNanos));
      lastCpuNanos = newCpuNanos;
    }
  }

  private class GcGenerationSpace
  {
    private final String name;
    private final MemoryUsage memoryUsage;

    public GcGenerationSpace(MemoryUsage memoryUsage, String name)
    {
      this.memoryUsage = memoryUsage;
      this.name = name;
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      builder.setDimension("gcGenSpaceName", name);

      emitter.emit(builder.build("jvm/gc/mem/max", memoryUsage.getMax()));
      emitter.emit(builder.build("jvm/gc/mem/capacity", memoryUsage.getCommitted()));
      emitter.emit(builder.build("jvm/gc/mem/used", memoryUsage.getUsed()));
      emitter.emit(builder.build("jvm/gc/mem/init", memoryUsage.getInit()));
    }
  }
}
