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

package org.apache.druid.segment.handoff;

import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Duration;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CoordinatorBasedSegmentHandoffNotifier implements SegmentHandoffNotifier
{
  private static final Logger log = new Logger(CoordinatorBasedSegmentHandoffNotifier.class);

  private final ConcurrentMap<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks = new ConcurrentHashMap<>();
  private final CoordinatorClient coordinatorClient;
  private volatile ScheduledExecutorService scheduledExecutor;
  private final Duration pollDuration;
  private final String dataSource;
  private final String taskId;

  public CoordinatorBasedSegmentHandoffNotifier(
      String dataSource,
      CoordinatorClient coordinatorClient,
      CoordinatorBasedSegmentHandoffNotifierConfig config,
      String taskId
  )
  {
    this.dataSource = dataSource;
    this.coordinatorClient = coordinatorClient;
    this.pollDuration = config.getPollDuration();
    this.taskId = taskId;
  }

  @Override
  public boolean registerSegmentHandoffCallback(SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable)
  {
    log.debug("Adding SegmentHandoffCallback for dataSource[%s] Segment[%s] for task[%s]", dataSource, descriptor, taskId);
    Pair<Executor, Runnable> prev = handOffCallbacks.putIfAbsent(
        descriptor,
        new Pair<>(exec, handOffRunnable)
    );
    return prev == null;
  }

  @Override
  public void start()
  {
    scheduledExecutor = Execs.scheduledSingleThreaded("coordinator_handoff_scheduled_%d");
    scheduledExecutor.scheduleAtFixedRate(
        this::checkForSegmentHandoffs,
        0L,
        pollDuration.getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  void checkForSegmentHandoffs()
  {
    try {
      Iterator<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> itr = handOffCallbacks.entrySet().iterator();

      while (itr.hasNext()) {
        Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry = itr.next();
        SegmentDescriptor descriptor = entry.getKey();
        try {
          Boolean handOffComplete =
              FutureUtils.getUnchecked(coordinatorClient.isHandoffComplete(dataSource, descriptor), true);
          if (Boolean.TRUE.equals(handOffComplete)) {
            log.debug("Segment handoff complete for dataSource[%s] segment[%s] for task[%s]", dataSource, descriptor, taskId);
            entry.getValue().lhs.execute(entry.getValue().rhs);
            itr.remove();
          }
        }
        catch (Exception e) {
          log.error(
              e,
              "Exception while checking handoff for dataSource[%s] Segment[%s], taskId[%s]; will try again after [%s]",
              dataSource,
              descriptor,
              taskId,
              pollDuration
          );
        }
      }
      if (!handOffCallbacks.isEmpty()) {
        log.info("Still waiting for handoff for [%d] segments for task[%s]", handOffCallbacks.size(), taskId);
      }
    }
    catch (Throwable t) {
      log.error(
          t,
          "Exception while checking handoff for dataSource[%s], taskId[%s]; will try again after [%s]",
          dataSource,
          taskId,
          pollDuration
      );
    }
  }

  @Override
  public void close()
  {
    scheduledExecutor.shutdown();
  }

  // Used in tests
  Map<SegmentDescriptor, Pair<Executor, Runnable>> getHandOffCallbacks()
  {
    return handOffCallbacks;
  }
}
