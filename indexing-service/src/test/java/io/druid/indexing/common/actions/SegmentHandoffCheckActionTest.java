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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.OverlordServerView;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NumberedPartitionChunk;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class SegmentHandoffCheckActionTest
{
  private TaskActionToolbox toolbox;
  private OverlordServerView serverView;
  private Task task;
  private VersionedIntervalTimeline<String, Set<DruidServerMetadata>> timeline;

  @Before
  public void setup()
  {
    toolbox = EasyMock.createNiceMock(TaskActionToolbox.class);
    serverView = EasyMock.createNiceMock(OverlordServerView.class);
    task = EasyMock.createNiceMock(Task.class);
    EasyMock.expect(toolbox.getServerView()).andReturn(serverView).anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("test_ds").anyTimes();
    timeline = new VersionedIntervalTimeline<>(
        Ordering.natural()
    );
    EasyMock.expect(serverView.getTimeline(new TableDataSource("test_ds"))).andReturn(timeline).anyTimes();
    EasyMock.replay(toolbox, serverView, task);
  }

  @After
  public void teardown()
  {
    EasyMock.verify(toolbox, serverView, task);
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-02"
            ), "v1", 2
        )
    );
    TaskAction serde = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(action), TaskAction.class
    );
    Assert.assertTrue(serde instanceof SegmentHandoffCheckAction);
    Assert.assertEquals(action, serde);
  }

  @Test
  public void testEmptyTimeline() throws IOException
  {
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-02"
            ), "v1", 2
        )
    );
    Assert.assertFalse(action.perform(task, toolbox));
  }

  @Test
  public void testSegmentOnRealtimeNode() throws IOException
  {
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-02"
            ), "v1", 2
        )
    );
    timeline.add(
        new Interval(
            "2011-04-01/2011-04-02"
        ),
        "v1",
        NumberedPartitionChunk.make(2, 3, (Set<DruidServerMetadata>) Sets.newHashSet(createRealtimeServerMetadata("a")))
    );
    Assert.assertFalse(action.perform(task, toolbox));
  }

  @Test
  public void testHandoffComplete() throws IOException
  {
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-02"
            ), "v1", 2
        )
    );
    timeline.add(
        new Interval(
            "2011-04-01/2011-04-02"
        ),
        "v1",
        NumberedPartitionChunk.make(
            2,
            3,
            (Set<DruidServerMetadata>) Sets.newHashSet(
                createRealtimeServerMetadata("a"),
                createHistoricalServerMetadata("b")
            )
        )
    );
    Assert.assertTrue(action.perform(task, toolbox));
  }

  @Test
  public void testOlderVersionSegmentHandedOff() throws IOException
  {
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-02"
            ), "v2", 2
        )
    );
    // historical has older version of segment
    timeline.add(
        new Interval(
            "2011-04-01/2011-04-02"
        ),
        "v1",
        NumberedPartitionChunk.make(
            2,
            3,
            (Set<DruidServerMetadata>) Sets.newHashSet(
                createRealtimeServerMetadata("a"),
                createHistoricalServerMetadata("b")
            )
        )
    );
    Assert.assertFalse(action.perform(task, toolbox));
  }

  @Test
  public void testHistoricalHasPartialInterval() throws IOException
  {
    SegmentHandoffCheckAction action = new SegmentHandoffCheckAction(
        new SegmentDescriptor(
            new Interval(
                "2011-04-01/2011-04-03"
            ), "v1", 2
        )
    );
    // historical segment with partial interval
    timeline.add(
        new Interval(
            "2011-04-01/2011-04-02"
        ),
        "v1",
        NumberedPartitionChunk.make(
            2,
            3,
            (Set<DruidServerMetadata>) Sets.newHashSet(
                createRealtimeServerMetadata("a"),
                createHistoricalServerMetadata("b")
            )
        )
    );
    Assert.assertFalse(action.perform(task, toolbox));
  }

  private DruidServerMetadata createRealtimeServerMetadata(String name)
  {
    return createServerMetadata(name, "realtime");
  }

  private DruidServerMetadata createHistoricalServerMetadata(String name)
  {
    return createServerMetadata(name, "historical");
  }

  private DruidServerMetadata createServerMetadata(String name, String type)
  {
    return new DruidServerMetadata(
        name,
        name,
        10000,
        type,
        "tier",
        1
    );
  }

}
