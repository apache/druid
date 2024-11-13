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
import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.jvm.gc.GcEvent;

import javax.annotation.Nullable;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
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

  private static final String JVM_VERSION = "jvmVersion";
  private static final String JAVA_VERSION = System.getProperty("java.version");
  private final JvmMonitorConfig config;

  @VisibleForTesting
  @Nullable
  final GcCollectors gcCollectors;
  private final Map<String, String[]> dimensions;
  @Nullable
  private final AllocationMetricCollector collector;
  @Nullable
  final GcGranularEventCollector gcEventCollector;

  public JvmMonitor(JvmMonitorConfig config)
  {
    this(ImmutableMap.of(), config);
  }

  public JvmMonitor(Map<String, String[]> dimensions, JvmMonitorConfig config)
  {
    this(dimensions, DEFAULT_METRICS_FEED, config);
  }

  public JvmMonitor(Map<String, String[]> dimensions, String feed, JvmMonitorConfig config)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.config = config;
    this.dimensions = ImmutableMap.copyOf(dimensions);
    this.collector = AllocationMetricCollectors.getAllocationMetricCollector();
    this.gcCollectors = new GcCollectors();

    this.gcEventCollector = config.recordDuration() ? new GcGranularEventCollector() : null;
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
    builder.setDimension(JVM_VERSION, JAVA_VERSION);
    if (collector != null) {
      long delta = collector.calculateDelta();
      emitter.emit(builder.setMetric("jvm/heapAlloc/bytes", delta));
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
      final ServiceMetricEvent.Builder builder = builder()
          .setDimension("memKind", kind)
          .setDimension(JVM_VERSION, JAVA_VERSION);
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.setMetric("jvm/mem/max", usage.getMax()));
      emitter.emit(builder.setMetric("jvm/mem/committed", usage.getCommitted()));
      emitter.emit(builder.setMetric("jvm/mem/used", usage.getUsed()));
      emitter.emit(builder.setMetric("jvm/mem/init", usage.getInit()));
    }

    // jvm/pool
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      final String kind = pool.getType() == MemoryType.HEAP ? "heap" : "nonheap";
      final MemoryUsage usage = pool.getUsage();
      final ServiceMetricEvent.Builder builder = builder()
          .setDimension("poolKind", kind)
          .setDimension("poolName", pool.getName())
          .setDimension(JVM_VERSION, JAVA_VERSION);
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.setMetric("jvm/pool/max", usage.getMax()));
      emitter.emit(builder.setMetric("jvm/pool/committed", usage.getCommitted()));
      emitter.emit(builder.setMetric("jvm/pool/used", usage.getUsed()));
      emitter.emit(builder.setMetric("jvm/pool/init", usage.getInit()));
    }
  }

  private void emitDirectMemMetrics(ServiceEmitter emitter)
  {
    for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
      final ServiceMetricEvent.Builder builder = builder()
          .setDimension("bufferpoolName", pool.getName())
          .setDimension(JVM_VERSION, JAVA_VERSION);
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);

      emitter.emit(builder.setMetric("jvm/bufferpool/capacity", pool.getTotalCapacity()));
      emitter.emit(builder.setMetric("jvm/bufferpool/used", pool.getMemoryUsed()));
      emitter.emit(builder.setMetric("jvm/bufferpool/count", pool.getCount()));
    }
  }

  private void emitGcMetrics(ServiceEmitter emitter)
  {
    gcCollectors.emit(emitter, dimensions);
    if (gcEventCollector != null) {
      gcEventCollector.emit(emitter, dimensions);
    }
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
    private final GcEvent event;
    private final GarbageCollectorMXBean gcBean;
    private long lastInvocations = 0;
    private long lastCpuMillis = 0;

    GcGenerationCollector(GarbageCollectorMXBean gcBean)
    {
      this.event = new GcEvent(gcBean.getName());
      this.gcBean = gcBean;
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      ImmutableMap.Builder<String, String[]> dimensionsCopyBuilder = ImmutableMap
          .<String, String[]>builder()
          .putAll(dimensions)
          .put("gcGen", new String[]{event.druidGenerationName});

      dimensionsCopyBuilder.put("gcName", new String[]{event.druidCollectorName});

      Map<String, String[]> dimensionsCopy = dimensionsCopyBuilder.build();

      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensionsCopy);
      builder.setDimension(JVM_VERSION, JAVA_VERSION);

      long newInvocations = gcBean.getCollectionCount();
      emitter.emit(builder.setMetric("jvm/gc/count", newInvocations - lastInvocations));
      lastInvocations = newInvocations;

      // getCollectionTime is in milliseconds; we report jvm/gc/cpu in nanoseconds.
      long newCpuMillis = gcBean.getCollectionTime();
      emitter.emit(builder.setMetric("jvm/gc/cpu", (newCpuMillis - lastCpuMillis) * 1_000_000L));
      lastCpuMillis = newCpuMillis;
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

      builder
          .setDimension(JVM_VERSION, JAVA_VERSION)
          .setDimension("gcGenSpaceName", name);

      emitter.emit(builder.setMetric("jvm/gc/mem/max", memoryUsage.getMax()));
      emitter.emit(builder.setMetric("jvm/gc/mem/capacity", memoryUsage.getCommitted()));
      emitter.emit(builder.setMetric("jvm/gc/mem/used", memoryUsage.getUsed()));
      emitter.emit(builder.setMetric("jvm/gc/mem/init", memoryUsage.getInit()));
    }
  }

  private class GcGranularEventCollector
  {
    // From: https://github.com/Netflix/spectator/blob/main/spectator-ext-gc/src/main/java/com/netflix/spectator/gc/GcLogger.java#L56
    private static final int BUFFER_SIZE = 256;
    private final EventBuffer<ServiceMetricEvent.Builder> buffer;
    private final GcNotificationListener listener;

    public GcGranularEventCollector()
    {
      this.buffer = new EventBuffer<>(ServiceMetricEvent.Builder.class, BUFFER_SIZE);
      this.listener = new GcNotificationListener();

      for (GarbageCollectorMXBean mbean : ManagementFactory.getGarbageCollectorMXBeans()) {
        if (mbean instanceof NotificationEmitter) {
          final NotificationEmitter emitter = (NotificationEmitter) mbean;
          emitter.addNotificationListener(this.listener, null, null);
        }
      }
    }

    void emit(ServiceEmitter emitter, Map<String, String[]> dimensions)
    {
      final ServiceMetricEvent.Builder[] events = buffer.extract();
      for (ServiceMetricEvent.Builder builder : events) {
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        emitter.emit(builder);
      }
    }

    private void processGcEvent(GarbageCollectionNotificationInfo info)
    {
      final ServiceMetricEvent.Builder builder = builder();

      final GcEvent event = new GcEvent(info.getGcName(), info.getGcCause());
      builder.setDimension("gcName", new String[]{event.druidCollectorName});
      builder.setDimension("gcGen", new String[]{event.druidGenerationName});
      builder.setDimension(JVM_VERSION, JAVA_VERSION);

      // record pause time or concurrent time
      final String durationMetricName = event.isConcurrent() ? "jvm/gc/concurrentTime" : "jvm/gc/pause";
      builder.setMetric(durationMetricName, info.getGcInfo().getDuration());
      buffer.push(builder);
    }

    private class GcNotificationListener implements NotificationListener
    {
      @Override
      public void handleNotification(Notification notification, Object ref)
      {
        final String type = notification.getType();
        if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(type)) {
          CompositeData cd = (CompositeData) notification.getUserData();
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(cd);
          processGcEvent(info);
        }
      }
    }
  }
}
