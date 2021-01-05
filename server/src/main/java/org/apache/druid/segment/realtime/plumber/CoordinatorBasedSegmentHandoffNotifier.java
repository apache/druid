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

package org.apache.druid.segment.realtime.plumber;

import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.coordination.DruidServerMetadata;

import java.util.Iterator;
import java.util.List;
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
  private final long pollDurationMillis;
  private final String dataSource;

  public CoordinatorBasedSegmentHandoffNotifier(
      String dataSource,
      CoordinatorClient coordinatorClient,
      CoordinatorBasedSegmentHandoffNotifierConfig config
  )
  {
    this.dataSource = dataSource;
    this.coordinatorClient = coordinatorClient;
    this.pollDurationMillis = config.getPollDuration().getMillis();
  }

  @Override
  public boolean registerSegmentHandoffCallback(SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable)
  {
    log.info("Adding SegmentHandoffCallback for dataSource[%s] Segment[%s]", dataSource, descriptor);
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
        new Runnable()
        {
          @Override
          public void run()
          {
            checkForSegmentHandoffs();
          }
        }, 0L, pollDurationMillis, TimeUnit.MILLISECONDS
    );
  }

  void checkForSegmentHandoffs()
  {
    try {
      Iterator<Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>>> itr = handOffCallbacks.entrySet()
                                                                                             .iterator();
      while (itr.hasNext()) {
        Map.Entry<SegmentDescriptor, Pair<Executor, Runnable>> entry = itr.next();
        SegmentDescriptor descriptor = entry.getKey();
        try {
          Boolean handOffComplete = coordinatorClient.isHandOffComplete(dataSource, descriptor);
          if (handOffComplete == null) {
            log.warn(
                "Failed to call the new coordinator API for checking segment handoff. Falling back to the old API"
            );
            final List<ImmutableSegmentLoadInfo> loadedSegments = coordinatorClient.fetchServerView(
                dataSource,
                descriptor.getInterval(),
                true
            );
            handOffComplete = isHandOffComplete(loadedSegments, descriptor);
          }
          if (handOffComplete) {
            log.info("Segment Handoff complete for dataSource[%s] Segment[%s]", dataSource, descriptor);
            entry.getValue().lhs.execute(entry.getValue().rhs);
            itr.remove();
          }
        }
        catch (Exception e) {
          log.error(
              e,
              "Exception while checking handoff for dataSource[%s] Segment[%s], Will try again after [%d]secs",
              dataSource,
              descriptor,
              pollDurationMillis
          );
        }
      }
      if (!handOffCallbacks.isEmpty()) {
        log.info("Still waiting for Handoff for Segments : [%s]", handOffCallbacks.keySet());
      }
    }
    catch (Throwable t) {
      log.error(
          t,
          "Exception while checking handoff for dataSource[%s], Will try again after [%d]secs",
          dataSource,
          pollDurationMillis
      );
    }
  }

  static boolean isHandOffComplete(List<ImmutableSegmentLoadInfo> serverView, SegmentDescriptor descriptor)
  {
    for (ImmutableSegmentLoadInfo segmentLoadInfo : serverView) {
      if (segmentLoadInfo.getSegment().getInterval().contains(descriptor.getInterval())
          && segmentLoadInfo.getSegment().getShardSpec().getPartitionNum()
             == descriptor.getPartitionNumber()
          && segmentLoadInfo.getSegment().getVersion().compareTo(descriptor.getVersion()) >= 0
          && segmentLoadInfo.getServers().stream().anyMatch(DruidServerMetadata::isSegmentReplicationOrBroadcastTarget)) {
        return true;
      }
    }
    return false;
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
