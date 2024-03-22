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

package org.apache.druid.indexing.appenderator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.UsedSegmentChecker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ActionBasedUsedSegmentCheckerTest
{
  @Test
  public void testBasic() throws IOException
  {
    final TaskActionClient taskActionClient = EasyMock.createMock(TaskActionClient.class);
    EasyMock.expect(
        taskActionClient.submit(
            new RetrieveUsedSegmentsAction("bar", ImmutableList.of(Intervals.of("2002/P1D")))
        )
    ).andReturn(
        ImmutableList.of(
            DataSegment.builder()
                       .dataSource("bar")
                       .interval(Intervals.of("2002/P1D"))
                       .shardSpec(new LinearShardSpec(0))
                       .version("b")
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource("bar")
                       .interval(Intervals.of("2002/P1D"))
                       .shardSpec(new LinearShardSpec(1))
                       .version("b")
                       .size(0)
                       .build()
        )
    );
    EasyMock.expect(
        taskActionClient.submit(
            new RetrieveUsedSegmentsAction(
                "foo",
                ImmutableList.of(Intervals.of("2000/P1D"), Intervals.of("2001/P1D"))
            )
        )
    ).andReturn(
        ImmutableList.of(
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(Intervals.of("2000/P1D"))
                       .shardSpec(new LinearShardSpec(0))
                       .version("a")
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(Intervals.of("2000/P1D"))
                       .shardSpec(new LinearShardSpec(1))
                       .version("a")
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(Intervals.of("2001/P1D"))
                       .shardSpec(new LinearShardSpec(1))
                       .version("b")
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(Intervals.of("2002/P1D"))
                       .shardSpec(new LinearShardSpec(1))
                       .version("b")
                       .size(0)
                       .build()
        )
    );
    EasyMock.replay(taskActionClient);

    final UsedSegmentChecker checker = new ActionBasedUsedSegmentChecker(taskActionClient);
    final Set<DataSegment> segments = checker.findUsedSegments(
        ImmutableSet.of(
            new SegmentIdWithShardSpec("foo", Intervals.of("2000/P1D"), "a", new LinearShardSpec(1)),
            new SegmentIdWithShardSpec("foo", Intervals.of("2001/P1D"), "b", new LinearShardSpec(0)),
            new SegmentIdWithShardSpec("bar", Intervals.of("2002/P1D"), "b", new LinearShardSpec(0))
        )
    );

    Assert.assertEquals(
        ImmutableSet.of(
            DataSegment.builder()
                       .dataSource("foo")
                       .interval(Intervals.of("2000/P1D"))
                       .shardSpec(new LinearShardSpec(1))
                       .version("a")
                       .size(0)
                       .build(),
            DataSegment.builder()
                       .dataSource("bar")
                       .interval(Intervals.of("2002/P1D"))
                       .shardSpec(new LinearShardSpec(0))
                       .version("b")
                       .size(0)
                       .build()
        ),
        segments
    );

    EasyMock.verify(taskActionClient);
  }
}
