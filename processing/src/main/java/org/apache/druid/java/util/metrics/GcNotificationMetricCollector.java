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
import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.apache.druid.java.util.common.logger.Logger;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects Spectator-style GC metrics via JMX GC notifications. Captures per-event
 * pause/concurrent phase durations, allocation rate, promotion rate, and live/max data sizes.
 */
public class GcNotificationMetricCollector implements NotificationListener
{
  private static final Logger log = new Logger(GcNotificationMetricCollector.class);

  private final AtomicLong allocationRateBytes = new AtomicLong(0);
  private final AtomicLong promotionRateBytes = new AtomicLong(0);
  private final AtomicLong liveDataSizeBytes = new AtomicLong(0);
  private final AtomicLong maxDataSizeBytes = new AtomicLong(0);
  private final ConcurrentLinkedQueue<GcPauseEvent> pauseEvents = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<GcPauseEvent> concurrentPhaseEvents = new ConcurrentLinkedQueue<>();

  // Tracks young gen size after previous GC for allocation rate calculation
  private volatile long youngGenSizeAfterPreviousGc = -1;

  private final List<GarbageCollectorMXBean> gcBeans;

  public GcNotificationMetricCollector()
  {
    this(ManagementFactory.getGarbageCollectorMXBeans());
  }

  @VisibleForTesting
  GcNotificationMetricCollector(List<GarbageCollectorMXBean> gcBeans)
  {
    this.gcBeans = gcBeans;
  }

  public void start()
  {
    for (GarbageCollectorMXBean bean : gcBeans) {
      if (bean instanceof NotificationEmitter) {
        ((NotificationEmitter) bean).addNotificationListener(this, null, null);
      }
    }
  }

  public void stop()
  {
    for (GarbageCollectorMXBean bean : gcBeans) {
      if (bean instanceof NotificationEmitter) {
        try {
          ((NotificationEmitter) bean).removeNotificationListener(this);
        }
        catch (ListenerNotFoundException e) {
          log.warn(e, "Failed to remove GC notification listener from [%s]", bean.getName());
        }
      }
    }
  }

  @Override
  public void handleNotification(Notification notification, Object handback)
  {
    if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
      return;
    }

    GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
        (javax.management.openmbean.CompositeData) notification.getUserData()
    );

    processGcEvent(info);
  }

  @VisibleForTesting
  void processGcEvent(GarbageCollectionNotificationInfo info)
  {
    GcInfo gcInfo = info.getGcInfo();
    Map<String, MemoryUsage> beforeUsages = gcInfo.getMemoryUsageBeforeGc();
    Map<String, MemoryUsage> afterUsages = gcInfo.getMemoryUsageAfterGc();

    long youngGenBefore = 0;
    long youngGenAfter = 0;
    long oldGenBefore = 0;
    long oldGenAfter = 0;
    long oldGenMax = 0;

    for (Map.Entry<String, MemoryUsage> entry : afterUsages.entrySet()) {
      String poolName = entry.getKey();
      MemoryUsage after = entry.getValue();
      MemoryUsage before = beforeUsages.get(poolName);

      if (isYoungGenPool(poolName)) {
        youngGenBefore += before != null ? before.getUsed() : 0;
        youngGenAfter += after.getUsed();
      } else if (isOldGenPool(poolName)) {
        oldGenBefore += before != null ? before.getUsed() : 0;
        oldGenAfter += after.getUsed();
        oldGenMax += after.getMax() > 0 ? after.getMax() : 0;
      }
    }

    // Allocation rate: youngGenBefore (this GC) - youngGenAfter (previous GC)
    long prevYoungGenAfter = youngGenSizeAfterPreviousGc;
    if (prevYoungGenAfter >= 0) {
      long allocated = youngGenBefore - prevYoungGenAfter;
      if (allocated > 0) {
        allocationRateBytes.addAndGet(allocated);
      }
    }
    youngGenSizeAfterPreviousGc = youngGenAfter;

    // Promotion rate: old gen growth during this GC
    long promoted = oldGenAfter - oldGenBefore;
    if (promoted > 0) {
      promotionRateBytes.addAndGet(promoted);
    }

    // Live data size and max data size: updated after major (old gen) GC
    if (!isConcurrentPhase(info)) {
      boolean isOldGc = isOldGenGc(info);
      if (isOldGc) {
        liveDataSizeBytes.set(oldGenAfter);
        maxDataSizeBytes.set(oldGenMax);
      }
    }

    // Record pause or concurrent phase event
    double durationSeconds = gcInfo.getDuration() / 1000.0;
    String gcAction = info.getGcAction();
    String gcCause = info.getGcCause();

    if (isConcurrentPhase(info)) {
      concurrentPhaseEvents.add(new GcPauseEvent(durationSeconds, gcAction, gcCause));
    } else {
      pauseEvents.add(new GcPauseEvent(durationSeconds, gcAction, gcCause));
    }
  }

  /**
   * Atomically drains accumulated metrics. Counters (allocationRate, promotionRate) are
   * reset to zero. Gauges (liveDataSize, maxDataSize) retain their values.
   */
  public DrainResult drain()
  {
    long allocRate = allocationRateBytes.getAndSet(0);
    long promoRate = promotionRateBytes.getAndSet(0);
    long liveData = liveDataSizeBytes.get();
    long maxData = maxDataSizeBytes.get();

    List<GcPauseEvent> pauses = new ArrayList<>();
    GcPauseEvent event;
    while ((event = pauseEvents.poll()) != null) {
      pauses.add(event);
    }

    List<GcPauseEvent> concurrentPhases = new ArrayList<>();
    while ((event = concurrentPhaseEvents.poll()) != null) {
      concurrentPhases.add(event);
    }

    return new DrainResult(
        allocRate,
        promoRate,
        liveData,
        maxData,
        Collections.unmodifiableList(pauses),
        Collections.unmodifiableList(concurrentPhases)
    );
  }

  @VisibleForTesting
  static boolean isYoungGenPool(String poolName)
  {
    String lower = poolName.toLowerCase(Locale.ROOT);
    return lower.contains("eden")
           || lower.endsWith("young gen")
           || "shenandoah".equals(lower)
           || "zheap".equals(lower);
  }

  @VisibleForTesting
  static boolean isOldGenPool(String poolName)
  {
    String lower = poolName.toLowerCase(Locale.ROOT);
    return lower.contains("old gen")
           || lower.contains("tenured")
           || "shenandoah".equals(lower)
           || "zheap".equals(lower);
  }

  @VisibleForTesting
  static boolean isConcurrentPhase(GarbageCollectionNotificationInfo info)
  {
    String gcCause = info.getGcCause();
    String gcName = info.getGcName();
    return "No GC".equals(gcCause)
           || "G1 Concurrent GC".equals(gcName)
           || gcName.endsWith("Cycles");
  }

  /**
   * Heuristic: old gen GC if cause is NOT "No GC" and the GC action contains "major"
   * or the GC name suggests an old generation collector.
   */
  private static boolean isOldGenGc(GarbageCollectionNotificationInfo info)
  {
    String action = info.getGcAction();
    String name = info.getGcName();
    return action.contains("major")
           || name.contains("Old")
           || name.contains("Tenured")
           || name.contains("MarkSweep")
           || "ZGC".equals(name)
           || "Shenandoah Cycles".equals(name);
  }

  /**
   * Holds the result of a single drain() call.
   */
  public static class DrainResult
  {
    private final long allocationRateBytes;
    private final long promotionRateBytes;
    private final long liveDataSizeBytes;
    private final long maxDataSizeBytes;
    private final List<GcPauseEvent> pauseEvents;
    private final List<GcPauseEvent> concurrentPhaseEvents;

    DrainResult(
        long allocationRateBytes,
        long promotionRateBytes,
        long liveDataSizeBytes,
        long maxDataSizeBytes,
        List<GcPauseEvent> pauseEvents,
        List<GcPauseEvent> concurrentPhaseEvents
    )
    {
      this.allocationRateBytes = allocationRateBytes;
      this.promotionRateBytes = promotionRateBytes;
      this.liveDataSizeBytes = liveDataSizeBytes;
      this.maxDataSizeBytes = maxDataSizeBytes;
      this.pauseEvents = pauseEvents;
      this.concurrentPhaseEvents = concurrentPhaseEvents;
    }

    public long getAllocationRateBytes()
    {
      return allocationRateBytes;
    }

    public long getPromotionRateBytes()
    {
      return promotionRateBytes;
    }

    public long getLiveDataSizeBytes()
    {
      return liveDataSizeBytes;
    }

    public long getMaxDataSizeBytes()
    {
      return maxDataSizeBytes;
    }

    public List<GcPauseEvent> getPauseEvents()
    {
      return pauseEvents;
    }

    public List<GcPauseEvent> getConcurrentPhaseEvents()
    {
      return concurrentPhaseEvents;
    }
  }

  /**
   * A single GC pause or concurrent phase event with duration and identifying dimensions.
   */
  public static class GcPauseEvent
  {
    private final double durationSeconds;
    private final String gcAction;
    private final String gcCause;

    public GcPauseEvent(double durationSeconds, String gcAction, String gcCause)
    {
      this.durationSeconds = durationSeconds;
      this.gcAction = gcAction;
      this.gcCause = gcCause;
    }

    public double getDurationSeconds()
    {
      return durationSeconds;
    }

    public String getGcAction()
    {
      return gcAction;
    }

    public String getGcCause()
    {
      return gcCause;
    }
  }
}
