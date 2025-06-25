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

package org.apache.druid.simulate.indexing;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.duty.DutySchedule;
import org.apache.druid.indexing.overlord.duty.OverlordDuty;
import org.apache.druid.indexing.overlord.duty.UnusedSegmentsKiller;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.joda.time.Period;

import java.util.Set;

/**
 * Extension module that registers a {@link QuickUnusedSegmentKiller}.
 * If this extension is used, {@code druid.manager.segments.killUnused.enabled}
 * must be set to false so that the default {@link UnusedSegmentsKiller} is not
 * triggered.
 */
public class QuickUnusedSegmentKillerModule implements DruidModule
{
  private boolean isOverlord;

  @Inject
  public void setNodeRole(@Self Set<NodeRole> roles)
  {
    this.isOverlord = roles.contains(NodeRole.OVERLORD);
  }

  @Override
  public void configure(Binder binder)
  {
    if (isOverlord) {
      Multibinder.newSetBinder(binder, OverlordDuty.class)
                 .addBinding()
                 .to(QuickUnusedSegmentKiller.class)
                 .in(LazySingleton.class);
    }
  }

  /**
   * Extends {@link UnusedSegmentsKiller} to set the following properties so that
   * unused segments are cleaned up immediately.
   * <ul>
   * <li>Duty period to 1 second</li>
   * <li>Buffer period to 10 millis</li>
   * </ul>
   */
  public static class QuickUnusedSegmentKiller extends UnusedSegmentsKiller
  {
    @Inject
    public QuickUnusedSegmentKiller(
        SegmentsMetadataManagerConfig config,
        TaskActionClientFactory taskActionClientFactory,
        IndexerMetadataStorageCoordinator storageCoordinator,
        @IndexingService DruidLeaderSelector leaderSelector,
        ScheduledExecutorFactory executorFactory,
        DataSegmentKiller dataSegmentKiller,
        GlobalTaskLockbox taskLockbox,
        ServiceEmitter emitter
    )
    {
      super(
          new SegmentsMetadataManagerConfig(
              config.getPollDuration(),
              config.getCacheUsageMode(),
              new UnusedSegmentKillerConfig(true, Period.millis(10))
          ),
          taskActionClientFactory,
          storageCoordinator,
          leaderSelector,
          executorFactory,
          dataSegmentKiller,
          taskLockbox,
          emitter
      );
    }

    @Override
    public DutySchedule getSchedule()
    {
      // Run duty every second. UnusedSegmentsKiller runs every hour
      return new DutySchedule(1_000, 1_000);
    }
  }
}
