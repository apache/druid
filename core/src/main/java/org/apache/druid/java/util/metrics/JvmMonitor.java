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

  private final Map<String, String[]> dimensions;

  @VisibleForTesting
  @Nullable
  final GcCollectors gcCollectors;

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
    this.gcCollectors = new GcCollectors();
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
    gcCollectors.emit(emitter, dimensions);
  }

  private class GcCollectors
  {
    private final List<GcGenerationCollector> generationCollectors = new ArrayList<>();
    private final List<GcSpaceCollector> spaceCollectors = new ArrayList<>();

    GcCollectors()
    {
      List<GarbageCollectorMXBean> collectorMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean collectorMxBean : collectorMxBeans) {
        generationCollectors.add(new GcGenerationCollector(collectorMxBean));
      }

      List<MemoryPoolMXBean> memoryPoolMxBeans = ManagementFactory.getMemoryPoolMXBeans();
      for (MemoryPoolMXBean memoryPoolMxBean : memoryPoolMxBeans) {
        MemoryUsage collectionUsage = memoryPoolMxBean.getCollectionUsage();
        if (collectionUsage != null) {
          spaceCollectors.add(new GcSpaceCollector(collectionUsage, memoryPoolMxBean.getName()));
        }
      }

    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      for (GcGenerationCollector generationCollector : generationCollectors) {
        generationCollector.emit(emitter, dimensions);
      }

      for (GcSpaceCollector spaceCollector : spaceCollectors) {
        spaceCollector.emit(emitter, dimensions);
      }
    }
  }

  private class GcGenerationCollector
  {
    private final String generation;
    private final String collectorName;
    private final GarbageCollectorMXBean gcBean;

    private long lastInvocations = 0;
    private long lastCpuNanos = 0;

    private static final String GC_YOUNG_GENERATION_NAME = "young";
    private static final String GC_OLD_GENERATION_NAME = "old";
    private static final String GC_ZGC_GENERATION_NAME = "zgc";

    private static final String CMS_COLLECTOR_NAME = "cms";
    private static final String G1_COLLECTOR_NAME = "g1";
    private static final String PARALLEL_COLLECTOR_NAME = "parallel";
    private static final String SERIAL_COLLECTOR_NAME = "serial";
    private static final String ZGC_COLLECTOR_NAME = "zgc";
    private static final String SHENANDOAN_COLLECTOR_NAME = "shenandoah";

    GcGenerationCollector(GarbageCollectorMXBean gcBean)
    {
      Pair<String, String> gcNamePair = getReadableName(gcBean.getName());
      this.generation = gcNamePair.lhs;
      this.collectorName = gcNamePair.rhs;
      this.gcBean = gcBean;
    }

    private Pair<String, String> getReadableName(String name)
    {
      switch (name) {
        //CMS
        case "ParNew":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, CMS_COLLECTOR_NAME);
        case "ConcurrentMarkSweep":
          return new Pair<>(GC_OLD_GENERATION_NAME, CMS_COLLECTOR_NAME);

        // G1
        case "G1 Young Generation":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, G1_COLLECTOR_NAME);
        case "G1 Old Generation":
          return new Pair<>(GC_OLD_GENERATION_NAME, G1_COLLECTOR_NAME);

        // Parallel
        case "PS Scavenge":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, PARALLEL_COLLECTOR_NAME);
        case "PS MarkSweep":
          return new Pair<>(GC_OLD_GENERATION_NAME, PARALLEL_COLLECTOR_NAME);

        // Serial
        case "Copy":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, SERIAL_COLLECTOR_NAME);
        case "MarkSweepCompact":
          return new Pair<>(GC_OLD_GENERATION_NAME, SERIAL_COLLECTOR_NAME);

        //zgc
        case "ZGC":
          return new Pair<>(GC_ZGC_GENERATION_NAME, ZGC_COLLECTOR_NAME);

        //Shenandoah
        case "Shenandoah Cycles":
          return new Pair<>(GC_YOUNG_GENERATION_NAME, SHENANDOAN_COLLECTOR_NAME);
        case "Shenandoah Pauses":
          return new Pair<>(GC_OLD_GENERATION_NAME, SHENANDOAN_COLLECTOR_NAME);

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

      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensionsCopy);

      long newInvocations = gcBean.getCollectionCount();
      emitter.emit(builder.build("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;

      long newCpuNanos = gcBean.getCollectionTime();
      emitter.emit(builder.build("jvm/gc/cpu", newCpuNanos - lastCpuNanos));
      lastCpuNanos = newCpuNanos;

    }
  }

  private class GcSpaceCollector
  {

    private final List<GcGenerationSpace> spaces = new ArrayList<>();

    public GcSpaceCollector(MemoryUsage collectionUsage, String name)
    {
      spaces.add(new GcGenerationSpace(collectionUsage, name));
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      for (GcGenerationSpace space : spaces) {
        space.emit(emitter, dimensions);
      }
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
