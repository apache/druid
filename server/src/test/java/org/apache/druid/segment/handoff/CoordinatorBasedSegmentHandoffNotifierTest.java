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

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.SegmentDescriptor;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class CoordinatorBasedSegmentHandoffNotifierTest
{

  private final CoordinatorBasedSegmentHandoffNotifierConfig notifierConfig = new CoordinatorBasedSegmentHandoffNotifierConfig()
  {
    @Override
    public Duration getPollDuration()
    {
      return Duration.millis(10);
    }
  };

  @Test
  public void testHandoffCallbackNotCalled()
  {
    Interval interval = Intervals.of("2011-04-01/2011-04-02");
    SegmentDescriptor descriptor = new SegmentDescriptor(interval, "v1", 2);

    CoordinatorClient coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    EasyMock.expect(coordinatorClient.isHandoffComplete("test_ds", descriptor))
            .andReturn(Futures.immediateFuture(false))
            .anyTimes();
    EasyMock.replay(coordinatorClient);
    CoordinatorBasedSegmentHandoffNotifier notifier = new CoordinatorBasedSegmentHandoffNotifier(
        "test_ds",
        coordinatorClient,
        notifierConfig,
        "test_task"
    );
    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    notifier.registerSegmentHandoffCallback(
        descriptor,
        Execs.directExecutor(),
        () -> callbackCalled.set(true)
    );
    notifier.checkForSegmentHandoffs();
    // callback should have registered
    Assert.assertEquals(1, notifier.getHandOffCallbacks().size());
    Assert.assertTrue(notifier.getHandOffCallbacks().containsKey(descriptor));
    Assert.assertFalse(callbackCalled.get());
    EasyMock.verify(coordinatorClient);
  }

  @Test
  public void testHandoffCallbackCalled()
  {
    Interval interval = Intervals.of("2011-04-01/2011-04-02");
    SegmentDescriptor descriptor = new SegmentDescriptor(interval, "v1", 2);

    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    CoordinatorClient coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    EasyMock.expect(coordinatorClient.isHandoffComplete("test_ds", descriptor))
            .andReturn(Futures.immediateFuture(true))
            .anyTimes();
    EasyMock.replay(coordinatorClient);
    CoordinatorBasedSegmentHandoffNotifier notifier = new CoordinatorBasedSegmentHandoffNotifier(
        "test_ds",
        coordinatorClient,
        notifierConfig,
        "test_task"
    );

    notifier.registerSegmentHandoffCallback(
        descriptor,
        Execs.directExecutor(),
        () -> callbackCalled.set(true)
    );
    Assert.assertEquals(1, notifier.getHandOffCallbacks().size());
    Assert.assertTrue(notifier.getHandOffCallbacks().containsKey(descriptor));
    notifier.checkForSegmentHandoffs();
    // callback should have been removed
    Assert.assertTrue(notifier.getHandOffCallbacks().isEmpty());
    Assert.assertTrue(callbackCalled.get());
    EasyMock.verify(coordinatorClient);
  }
}
