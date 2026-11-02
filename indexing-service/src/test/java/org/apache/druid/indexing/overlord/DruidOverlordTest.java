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

package org.apache.druid.indexing.overlord;

import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.indexing.common.actions.SegmentAllocationQueue;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.NoopTaskContextEnricher;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.duty.OverlordDutyExecutor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class DruidOverlordTest
{
  @Test
  public void testStandaloneOverlordStartsAndStopsSegmentMetadataCache()
  {
    final SegmentMetadataCache segmentMetadataCache = EasyMock.createStrictMock(SegmentMetadataCache.class);
    segmentMetadataCache.start();
    EasyMock.expectLastCall().once();
    segmentMetadataCache.stop();
    EasyMock.expectLastCall().once();
    EasyMock.replay(segmentMetadataCache);

    final DruidOverlord overlord = createOverlord(OverlordMode.STANDALONE, segmentMetadataCache);
    overlord.start();
    overlord.stop();

    EasyMock.verify(segmentMetadataCache);
  }

  @Test
  public void testOverlordEmbeddedInCoordinatorDoesNotStartSegmentMetadataCache()
  {
    // The Coordinator owns the lifecycle of the shared cache when the Overlord is embedded in it
    final SegmentMetadataCache segmentMetadataCache = EasyMock.createStrictMock(SegmentMetadataCache.class);
    EasyMock.replay(segmentMetadataCache);

    final DruidOverlord overlord = createOverlord(OverlordMode.EMBEDDED_IN_COORDINATOR, segmentMetadataCache);
    overlord.start();
    overlord.stop();

    EasyMock.verify(segmentMetadataCache);
  }

  private static DruidOverlord createOverlord(
      final OverlordMode overlordMode,
      final SegmentMetadataCache segmentMetadataCache
  )
  {
    return new DruidOverlord(
        niceMock(TaskMaster.class),
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null, null),
        new DefaultTaskConfig(),
        niceMock(GlobalTaskLockbox.class),
        niceMock(TaskStorage.class),
        niceMock(TaskActionClientFactory.class),
        niceMock(TaskRunnerFactory.class),
        overlordMode,
        niceMock(ServiceEmitter.class),
        niceMock(SupervisorManager.class),
        niceMock(OverlordDutyExecutor.class),
        niceMock(DruidLeaderSelector.class),
        niceMock(SegmentAllocationQueue.class),
        segmentMetadataCache,
        Set.of(),
        new DefaultObjectMapper(),
        new NoopTaskContextEnricher()
    );
  }

  private static <T> T niceMock(final Class<T> clazz)
  {
    final T mock = EasyMock.createNiceMock(clazz);
    EasyMock.replay(mock);
    return mock;
  }
}
