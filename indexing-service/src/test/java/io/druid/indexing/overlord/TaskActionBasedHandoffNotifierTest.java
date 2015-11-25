/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.google.common.util.concurrent.MoreExecutors;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentHandoffCheckAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.SegmentDescriptor;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TaskActionBasedHandoffNotifierTest
{

  TaskActionBasedHandoffNotifierConfig notifierConfig = new TaskActionBasedHandoffNotifierConfig()
  {
    @Override
    public Duration getPollDuration()
    {
      return Duration.millis(10);
    }
  };

  @Test
  public void testHandoffCallbackNotCalled() throws IOException, InterruptedException
  {
    SegmentDescriptor descriptor = new SegmentDescriptor(
        new Interval(
            "2011-04-01/2011-04-02"
        ), "v1", 2
    );

    TaskActionClient client = EasyMock.createMock(TaskActionClient.class);
    EasyMock.expect(client.submit(new SegmentHandoffCheckAction(descriptor))).andReturn(false).anyTimes();
    EasyMock.replay(client);
    TaskActionBasedHandoffNotifier notifier = new TaskActionBasedHandoffNotifier(client, notifierConfig);
    notifier.start();
    final CountDownLatch handOffLatch = new CountDownLatch(1);

    notifier.registerSegmentHandoffCallback(
        descriptor, MoreExecutors.sameThreadExecutor(), new Runnable()
        {
          @Override
          public void run()
          {
            handOffLatch.countDown();
          }
        }
    );

    // callback should have registered
    Assert.assertEquals(1, notifier.getHandOffCallbacks().size());
    Assert.assertTrue(notifier.getHandOffCallbacks().containsKey(descriptor));
    Assert.assertFalse(handOffLatch.await(100, TimeUnit.MILLISECONDS));
    notifier.stop();
    EasyMock.verify(client);
  }

  @Test
  public void testHandoffCallbackCalled() throws IOException, InterruptedException
  {
    SegmentDescriptor descriptor = new SegmentDescriptor(
        new Interval(
            "2011-04-01/2011-04-02"
        ), "v1", 2
    );

    TaskActionClient client = EasyMock.createMock(TaskActionClient.class);
    EasyMock.expect(client.submit(new SegmentHandoffCheckAction(descriptor))).andReturn(true).anyTimes();
    EasyMock.replay(client);
    TaskActionBasedHandoffNotifier notifier = new TaskActionBasedHandoffNotifier(
        client,
        notifierConfig
    );
    notifier.start();
    final CountDownLatch handOffLatch = new CountDownLatch(1);

    notifier.registerSegmentHandoffCallback(
        descriptor, MoreExecutors.sameThreadExecutor(), new Runnable()
        {
          @Override
          public void run()
          {
            handOffLatch.countDown();
          }
        }
    );

    // callback should have been called.
    Assert.assertTrue(handOffLatch.await(400, TimeUnit.MILLISECONDS));
    Assert.assertEquals(0, notifier.getHandOffCallbacks().size());
    notifier.stop();
    EasyMock.verify(client);
  }

}
