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

package org.apache.druid.indexing.overlord.supervisor;

import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.Future;

public class StreamSupervisorTest
{
  @Test
  public void testDefaultHandoffTaskGroupsEarly()
  {
    // Create an instance of stream supervisor without overriding handoffTaskGroupsEarly().
    final StreamSupervisor streamSupervisor = new StreamSupervisor()
    {

      @Override
      public void start()
      {

      }

      @Override
      public void stop(boolean stopGracefully)
      {

      }

      @Override
      public SupervisorReport getStatus()
      {
        return null;
      }

      @Override
      public SupervisorStateManager.State getState()
      {
        return null;
      }

      @Override
      public void reset(@Nullable DataSourceMetadata dataSourceMetadata)
      {

      }

      @Override
      public void resetOffsets(DataSourceMetadata resetDataSourceMetadata)
      {

      }

      @Override
      public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
      {

      }

      @Override
      public LagStats computeLagStats()
      {
        return null;
      }

      @Override
      public int getActiveTaskGroupsCount()
      {
        return 0;
      }
    };

    final Exception ex = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> streamSupervisor.handoffTaskGroupsEarly(ImmutableList.of(1))
    );
    Assert.assertEquals(
        "Supervisor does not have the feature to handoff task groups early implemented",
        ex.getMessage()
    );
  }

  @Test
  public void testDefaultStopAsync()
  {
    // Create an instance of stream supervisor without overriding stopAsync().
    final StreamSupervisor streamSupervisor = new StreamSupervisor()
    {
      private SupervisorStateManager.State state = SupervisorStateManager.BasicState.RUNNING;

      @Override
      public void start()
      {

      }

      @Override
      public void stop(boolean stopGracefully)
      {
        state = SupervisorStateManager.BasicState.STOPPING;
      }

      @Override
      public SupervisorReport getStatus()
      {
        return null;
      }

      @Override
      public SupervisorStateManager.State getState()
      {
        return state;
      }

      @Override
      public void reset(@Nullable DataSourceMetadata dataSourceMetadata)
      {

      }

      @Override
      public void resetOffsets(DataSourceMetadata resetDataSourceMetadata)
      {

      }

      @Override
      public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
      {

      }

      @Override
      public LagStats computeLagStats()
      {
        return null;
      }

      @Override
      public int getActiveTaskGroupsCount()
      {
        return 0;
      }
    };

    Future<Void> stopAsyncFuture = streamSupervisor.stopAsync();
    Assert.assertTrue(stopAsyncFuture.isDone());

    // stop should be called by stopAsync
    Assert.assertEquals(SupervisorStateManager.BasicState.STOPPING, streamSupervisor.getState());
  }
}
