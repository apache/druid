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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.DateTimes;

import javax.annotation.Nullable;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Collects a live snapshot of the Java platform threads running in the current Druid process.
 *
 * <p>This class intentionally has no instance state. A new collector can be created for each request.
 */
public class StackTraceCollector
{
  public static final String MAX_STACK_TRACE_FRAME_DEPTH_KEY = "maxStackTraceFrameDepth";
  public static final int MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH = 10;
  public static final int DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH = 100;
  public static final int MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH = 1000;

  public ThreadStackTraceResponse collect()
  {
    return collect(DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH);
  }

  public ThreadStackTraceResponse collect(final int maxStackTraceFrameDepth)
  {
    validateMaxStackTraceFrameDepth(maxStackTraceFrameDepth);
    final String collectedAt = DateTimes.nowUtc().toString();
    final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
    final boolean cpuTimeEnabled = isCpuTimeEnabled(threadMxBean);
    final Set<Long> deadlockedThreadIds = findDeadlockedThreadIds(threadMxBean);
    final long[] threadIds = threadMxBean.getAllThreadIds();
    final ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(
        threadIds,
        threadMxBean.isObjectMonitorUsageSupported(),
        threadMxBean.isSynchronizerUsageSupported(),
        maxStackTraceFrameDepth
    );
    final List<ThreadStackTrace> threads = new ArrayList<>(threadInfos.length);

    for (final ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null) {
        continue;
      }

      final long threadId = threadInfo.getThreadId();
      final long rawLockOwnerId = threadInfo.getLockOwnerId();
      final Long lockOwnerId = rawLockOwnerId < 0 ? null : rawLockOwnerId;
      threads.add(
          new ThreadStackTrace(
              threadId,
              threadInfo.getThreadName(),
              threadInfo.getThreadState().name(),
              threadInfo.isDaemon(),
              threadInfo.getPriority(),
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, false),
              getThreadCpuTime(threadMxBean, threadId, cpuTimeEnabled, true),
              threadInfo.getLockName(),
              lockOwnerId,
              threadInfo.getLockOwnerName(),
              deadlockedThreadIds.contains(threadId),
              formatThreadInfo(threadInfo)
          )
      );
    }

    return new ThreadStackTraceResponse(collectedAt, threads);
  }

  public static int parseMaxStackTraceFrameDepth(@Nullable final String value)
  {
    if (value == null) {
      return DEFAULT_MAX_STACK_TRACE_FRAME_DEPTH;
    }

    try {
      return validateMaxStackTraceFrameDepth(Long.parseLong(value));
    }
    catch (NumberFormatException e) {
      throw InvalidInput.exception(
          "Query parameter[%s] must be an integer, but got[%s]",
          MAX_STACK_TRACE_FRAME_DEPTH_KEY,
          value
      );
    }
  }

  public static int validateMaxStackTraceFrameDepth(final long maxStackTraceFrameDepth)
  {
    InvalidInput.conditionalException(
        maxStackTraceFrameDepth >= MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        "[%s] must be greater than or equal to %d, but got[%d]",
        MAX_STACK_TRACE_FRAME_DEPTH_KEY,
        MIN_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        maxStackTraceFrameDepth
    );
    InvalidInput.conditionalException(
        maxStackTraceFrameDepth <= MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        "[%s] must be less than or equal to %d, but got[%d]",
        MAX_STACK_TRACE_FRAME_DEPTH_KEY,
        MAX_ALLOWED_STACK_TRACE_FRAME_DEPTH,
        maxStackTraceFrameDepth
    );
    return (int) maxStackTraceFrameDepth;
  }

  /**
   * Formats a thread stack in a jstack-style format based on {@link ThreadInfo#toString()}, but
   * includes all frames returned by the MX bean. {@code ThreadInfo.toString()} intentionally limits
   * its output to eight frames and appends an ellipsis.
   */
  private static String formatThreadInfo(final ThreadInfo threadInfo)
  {
    final StringBuilder builder = new StringBuilder();
    builder.append('"')
           .append(threadInfo.getThreadName())
           .append('"')
           .append(threadInfo.isDaemon() ? " daemon" : "")
           .append(" prio=")
           .append(threadInfo.getPriority())
           .append(" Id=")
           .append(threadInfo.getThreadId())
           .append(' ')
           .append(threadInfo.getThreadState());

    if (threadInfo.getLockName() != null) {
      builder.append(" on ").append(threadInfo.getLockName());
    }
    if (threadInfo.getLockOwnerName() != null) {
      builder.append(" owned by \"")
             .append(threadInfo.getLockOwnerName())
             .append("\" Id=")
             .append(threadInfo.getLockOwnerId());
    }
    if (threadInfo.isSuspended()) {
      builder.append(" (suspended)");
    }
    if (threadInfo.isInNative()) {
      builder.append(" (in native)");
    }
    builder.append('\n');

    final StackTraceElement[] stackTrace = threadInfo.getStackTrace();
    final MonitorInfo[] lockedMonitors = threadInfo.getLockedMonitors();
    for (int i = 0; i < stackTrace.length; i++) {
      builder.append("\tat ").append(stackTrace[i]);

      if (i == 0) {
        final LockInfo lockInfo = threadInfo.getLockInfo();
        if (lockInfo != null) {
          switch (threadInfo.getThreadState()) {
            case BLOCKED:
              builder.append(" - blocked on ").append(lockInfo);
              break;
            case WAITING:
            case TIMED_WAITING:
              builder.append(" - waiting on ").append(lockInfo);
              break;
            default:
              break;
          }
        }
      }

      builder.append('\n');

      for (final MonitorInfo monitorInfo : lockedMonitors) {
        if (monitorInfo.getLockedStackDepth() == i) {
          builder.append("\t-  locked ").append(monitorInfo).append('\n');
        }
      }
    }

    final LockInfo[] lockedSynchronizers = threadInfo.getLockedSynchronizers();
    if (lockedSynchronizers.length > 0) {
      builder.append("\n\tNumber of locked synchronizers = ")
             .append(lockedSynchronizers.length)
             .append('\n');
      for (final LockInfo lockedSynchronizer : lockedSynchronizers) {
        builder.append("\t- ").append(lockedSynchronizer).append('\n');
      }
    }

    return builder.append('\n').toString();
  }

  private static boolean isCpuTimeEnabled(final ThreadMXBean threadMxBean)
  {
    try {
      return threadMxBean.isThreadCpuTimeSupported() && threadMxBean.isThreadCpuTimeEnabled();
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return false;
    }
  }

  @Nullable
  private static Long getThreadCpuTime(
      final ThreadMXBean threadMxBean,
      final long threadId,
      final boolean cpuTimeEnabled,
      final boolean userTime
  )
  {
    if (!cpuTimeEnabled) {
      return null;
    }

    try {
      final long cpuTime = userTime
                           ? threadMxBean.getThreadUserTime(threadId)
                           : threadMxBean.getThreadCpuTime(threadId);
      return cpuTime < 0 ? null : cpuTime;
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return null;
    }
  }

  private static Set<Long> findDeadlockedThreadIds(final ThreadMXBean threadMxBean)
  {
    try {
      final long[] threadIds = threadMxBean.findDeadlockedThreads();
      if (threadIds == null) {
        return Collections.emptySet();
      }

      final Set<Long> deadlockedThreadIds = new HashSet<>();
      for (final long threadId : threadIds) {
        deadlockedThreadIds.add(threadId);
      }
      return deadlockedThreadIds;
    }
    catch (UnsupportedOperationException | SecurityException e) {
      return Collections.emptySet();
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ThreadStackTraceResponse
  {
    private final String collectedAt;
    private final List<ThreadStackTrace> threads;

    @JsonCreator
    public ThreadStackTraceResponse(
        @JsonProperty("collectedAt") final String collectedAt,
        @JsonProperty("threads") final List<ThreadStackTrace> threads
    )
    {
      this.collectedAt = collectedAt;
      this.threads = threads == null ? ImmutableList.of() : ImmutableList.copyOf(threads);
    }

    @JsonProperty
    public String getCollectedAt()
    {
      return collectedAt;
    }

    @JsonProperty
    public List<ThreadStackTrace> getThreads()
    {
      return threads;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ThreadStackTrace
  {
    private final long threadId;
    private final String threadName;
    private final String threadState;
    private final boolean daemon;
    private final int priority;
    @Nullable
    private final Long cpuTimeNs;
    @Nullable
    private final Long userCpuTimeNs;
    @Nullable
    private final String lockName;
    @Nullable
    private final Long lockOwnerId;
    @Nullable
    private final String lockOwnerName;
    private final boolean deadlocked;
    private final String stackTrace;

    @JsonCreator
    public ThreadStackTrace(
        @JsonProperty("threadId") final long threadId,
        @JsonProperty("threadName") final String threadName,
        @JsonProperty("threadState") final String threadState,
        @JsonProperty("daemon") final boolean daemon,
        @JsonProperty("priority") final int priority,
        @JsonProperty("cpuTimeNs") @Nullable final Long cpuTimeNs,
        @JsonProperty("userCpuTimeNs") @Nullable final Long userCpuTimeNs,
        @JsonProperty("lockName") @Nullable final String lockName,
        @JsonProperty("lockOwnerId") @Nullable final Long lockOwnerId,
        @JsonProperty("lockOwnerName") @Nullable final String lockOwnerName,
        @JsonProperty("deadlocked") final boolean deadlocked,
        @JsonProperty("stackTrace") final String stackTrace
    )
    {
      this.threadId = threadId;
      this.threadName = threadName;
      this.threadState = threadState;
      this.daemon = daemon;
      this.priority = priority;
      this.cpuTimeNs = cpuTimeNs;
      this.userCpuTimeNs = userCpuTimeNs;
      this.lockName = lockName;
      this.lockOwnerId = lockOwnerId;
      this.lockOwnerName = lockOwnerName;
      this.deadlocked = deadlocked;
      this.stackTrace = stackTrace;
    }

    @JsonProperty
    public long getThreadId()
    {
      return threadId;
    }

    @JsonProperty
    public String getThreadName()
    {
      return threadName;
    }

    @JsonProperty
    public String getThreadState()
    {
      return threadState;
    }

    @JsonProperty
    public boolean isDaemon()
    {
      return daemon;
    }

    @JsonProperty
    public int getPriority()
    {
      return priority;
    }

    @JsonProperty
    public Long getCpuTimeNs()
    {
      return cpuTimeNs;
    }

    @JsonProperty
    public Long getUserCpuTimeNs()
    {
      return userCpuTimeNs;
    }

    @JsonProperty
    public String getLockName()
    {
      return lockName;
    }

    @JsonProperty
    public Long getLockOwnerId()
    {
      return lockOwnerId;
    }

    @JsonProperty
    public String getLockOwnerName()
    {
      return lockOwnerName;
    }

    @JsonProperty
    public boolean isDeadlocked()
    {
      return deadlocked;
    }

    @JsonProperty
    public String getStackTrace()
    {
      return stackTrace;
    }
  }
}
