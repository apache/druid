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

package org.apache.druid.testing.embedded;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliBroker;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.ServerView;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Embedded mode of {@link CliBroker} used in embedded tests.
 * Add this to your {@link EmbeddedDruidCluster} if you want to run queries
 * against it.
 */
public class EmbeddedBroker extends EmbeddedDruidServer
{
  private final SegmentAvailabilityTracker availabilityTracker = new SegmentAvailabilityTracker();

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Broker(handler);
  }

  /**
   * Waits until the provided collection of segments is available for querying.
   */
  public void waitForSegmentAvailability(final Collection<SegmentId> segmentIds)
  {
    try {
      availabilityTracker.waitForSegmentAvailability(segmentIds);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private class Broker extends CliBroker
  {
    private final LifecycleInitHandler handler;

    private Broker(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final BrokerServerView serverView = injector.getInstance(BrokerServerView.class);
      serverView.registerSegmentCallback(Execs.directExecutor(), availabilityTracker);

      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(super.getModules());
      modules.add(EmbeddedBroker.this::bindReferenceHolder);
      return modules;
    }
  }

  private static class SegmentAvailabilityTracker implements ServerView.SegmentCallback
  {
    @GuardedBy("itself")
    private final Set<SegmentId> segments = new HashSet<>();

    public void waitForSegmentAvailability(Collection<SegmentId> desiredSegments) throws InterruptedException
    {
      final Set<SegmentId> remainingSegments = Sets.newHashSet(desiredSegments);
      while (!remainingSegments.isEmpty()) {
        synchronized (segments) {
          remainingSegments.removeAll(segments);
          if (remainingSegments.isEmpty()) {
            return;
          }

          segments.wait();
        }
      }
    }

    @Override
    public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      synchronized (segments) {
        segments.add(segment.getId());
        segments.notifyAll();
      }

      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      synchronized (segments) {
        segments.remove(segment.getId());
        segments.notifyAll();
      }

      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentViewInitialized()
    {
      return ServerView.CallbackAction.CONTINUE;
    }

    @Override
    public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
    {
      return ServerView.CallbackAction.CONTINUE;
    }
  }
}
