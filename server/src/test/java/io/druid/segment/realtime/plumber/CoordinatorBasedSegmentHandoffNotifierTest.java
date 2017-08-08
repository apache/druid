/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.client.coordinator.CoordinatorClient;
import io.druid.query.SegmentDescriptor;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NumberedShardSpec;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.IOException;
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
  public void testHandoffCallbackNotCalled() throws IOException, InterruptedException
  {
    Interval interval = new Interval(
        "2011-04-01/2011-04-02"
    );
    SegmentDescriptor descriptor = new SegmentDescriptor(
        interval, "v1", 2
    );
    DataSegment segment = new DataSegment(
        "test_ds",
        interval,
        "v1",
        null,
        null,
        null,
        new NumberedShardSpec(2, 3),
        0, 0
    );

    CoordinatorClient coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    EasyMock.expect(coordinatorClient.fetchServerView("test_ds", interval, true))
            .andReturn(
                Lists.newArrayList(
                    new ImmutableSegmentLoadInfo(
                        segment,
                        Sets.newHashSet(
                            createRealtimeServerMetadata("a1")
                        )
                    )
                )
            ).anyTimes();
    EasyMock.replay(coordinatorClient);
    CoordinatorBasedSegmentHandoffNotifier notifier = new CoordinatorBasedSegmentHandoffNotifier(
        "test_ds",
        coordinatorClient,
        notifierConfig
    );
    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    notifier.registerSegmentHandoffCallback(
        descriptor, MoreExecutors.sameThreadExecutor(), new Runnable()
        {
          @Override
          public void run()
          {
            callbackCalled.set(true);
          }
        }
    );
    notifier.checkForSegmentHandoffs();
    // callback should have registered
    Assert.assertEquals(1, notifier.getHandOffCallbacks().size());
    Assert.assertTrue(notifier.getHandOffCallbacks().containsKey(descriptor));
    Assert.assertFalse(callbackCalled.get());
    EasyMock.verify(coordinatorClient);
  }

  @Test
  public void testHandoffCallbackCalled() throws IOException, InterruptedException
  {
    Interval interval = new Interval(
        "2011-04-01/2011-04-02"
    );
    SegmentDescriptor descriptor = new SegmentDescriptor(
        interval, "v1", 2
    );
    DataSegment segment = new DataSegment(
        "test_ds",
        interval,
        "v1",
        null,
        null,
        null,
        new NumberedShardSpec(2, 3),
        0, 0
    );
    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    CoordinatorClient coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    EasyMock.expect(coordinatorClient.fetchServerView("test_ds", interval, true))
            .andReturn(
                Lists.newArrayList(
                    new ImmutableSegmentLoadInfo(
                        segment,
                        Sets.newHashSet(
                            createHistoricalServerMetadata("a1")
                        )
                    )
                )
            ).anyTimes();
    EasyMock.replay(coordinatorClient);
    CoordinatorBasedSegmentHandoffNotifier notifier = new CoordinatorBasedSegmentHandoffNotifier(
        "test_ds",
        coordinatorClient,
        notifierConfig
    );

    notifier.registerSegmentHandoffCallback(
        descriptor, MoreExecutors.sameThreadExecutor(), new Runnable()
        {
          @Override
          public void run()
          {
            callbackCalled.set(true);
          }
        }
    );
    Assert.assertEquals(1, notifier.getHandOffCallbacks().size());
    Assert.assertTrue(notifier.getHandOffCallbacks().containsKey(descriptor));
    notifier.checkForSegmentHandoffs();
    // callback should have been removed
    Assert.assertTrue(notifier.getHandOffCallbacks().isEmpty());
    Assert.assertTrue(callbackCalled.get());
    EasyMock.verify(coordinatorClient);
  }

  @Test
  public void testHandoffChecksForVersion()
  {
    Interval interval = new Interval(
        "2011-04-01/2011-04-02"
    );
    Assert.assertFalse(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v2", 2)
        )
    );

    Assert.assertTrue(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v2", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

    Assert.assertTrue(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

  }

  @Test
  public void testHandoffChecksForAssignableServer()
  {
    Interval interval = new Interval(
        "2011-04-01/2011-04-02"
    );
    Assert.assertTrue(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

    Assert.assertFalse(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 2),
                    Sets.newHashSet(createRealtimeServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );
  }

  @Test
  public void testHandoffChecksForPartitionNumber()
  {
    Interval interval = new Interval(
        "2011-04-01/2011-04-02"
    );
    Assert.assertTrue(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 1)
        )
    );

    Assert.assertFalse(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(interval, "v1", 1),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(interval, "v1", 2)
        )
    );

  }

  @Test
  public void testHandoffChecksForInterval()
  {

    Assert.assertFalse(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(
                        new Interval(
                            "2011-04-01/2011-04-02"
                        ), "v1", 1
                    ),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(
                new Interval(
                    "2011-04-01/2011-04-03"
                ), "v1", 1
            )
        )
    );

    Assert.assertTrue(
        CoordinatorBasedSegmentHandoffNotifier.isHandOffComplete(
            Lists.newArrayList(
                new ImmutableSegmentLoadInfo(
                    createSegment(
                        new Interval(
                            "2011-04-01/2011-04-04"
                        ), "v1", 1
                    ),
                    Sets.newHashSet(createHistoricalServerMetadata("a"))
                )
            ),
            new SegmentDescriptor(
                new Interval(
                    "2011-04-02/2011-04-03"
                ), "v1", 1
            )
        )
    );
  }

  private DruidServerMetadata createRealtimeServerMetadata(String name)
  {
    return createServerMetadata(name, ServerType.REALTIME);
  }

  private DruidServerMetadata createHistoricalServerMetadata(String name)
  {
    return createServerMetadata(name, ServerType.HISTORICAL);
  }

  private DruidServerMetadata createServerMetadata(String name, ServerType type)
  {
    return new DruidServerMetadata(
        name,
        name,
        null,
        10000,
        type,
        "tier",
        1
    );
  }

  private DataSegment createSegment(Interval interval, String version, int partitionNumber)
  {
    return new DataSegment(
        "test_ds",
        interval,
        version,
        null,
        null,
        null,
        new NumberedShardSpec(partitionNumber, 100),
        0, 0
    );
  }
}
