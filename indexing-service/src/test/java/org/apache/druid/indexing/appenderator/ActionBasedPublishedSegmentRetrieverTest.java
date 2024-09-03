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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.actions.RetrieveSegmentsByIdAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ActionBasedPublishedSegmentRetrieverTest
{
  private TaskActionClient taskActionClient;
  private ActionBasedPublishedSegmentRetriever segmentRetriever;

  @Before
  public void setup()
  {
    taskActionClient = EasyMock.createMock(TaskActionClient.class);
    segmentRetriever = new ActionBasedPublishedSegmentRetriever(taskActionClient);
  }

  @Test
  public void testRetrieveSegmentsById() throws IOException
  {
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(3, Granularities.DAY)
                          .startingAt("2013-01-01")
                          .eachOfSizeInMb(400);

    EasyMock.expect(
        taskActionClient.submit(
            new RetrieveSegmentsByIdAction(
                TestDataSource.WIKI,
                segments.stream().map(segment -> segment.getId().toString()).collect(Collectors.toSet())
            )
        )
    ).andReturn(new HashSet<>(segments)).once();
    EasyMock.replay(taskActionClient);

    final Set<SegmentId> searchSegmentIds = segments.stream()
                                                    .map(DataSegment::getId)
                                                    .collect(Collectors.toSet());
    Assert.assertEquals(
        new HashSet<>(segments),
        segmentRetriever.findPublishedSegments(searchSegmentIds)
    );

    EasyMock.verify(taskActionClient);
  }

  @Test
  public void testRetrieveUsedSegmentsIfNotFoundById() throws IOException
  {
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(3, Granularities.DAY)
                          .startingAt("2013-01-01")
                          .eachOfSizeInMb(400);

    EasyMock.expect(
        taskActionClient.submit(
            new RetrieveSegmentsByIdAction(TestDataSource.WIKI, EasyMock.anyObject())
        )
    ).andThrow(InvalidInput.exception("task action not supported yet")).once();
    EasyMock.expect(
        taskActionClient.submit(
            new RetrieveUsedSegmentsAction(
                TestDataSource.WIKI,
                Collections.singletonList(Intervals.of("2013-01-01/P3D")),
                Segments.INCLUDING_OVERSHADOWED
            )
        )
    ).andReturn(segments).once();
    EasyMock.replay(taskActionClient);

    final Set<SegmentId> searchSegmentIds = segments.stream()
                                                    .map(DataSegment::getId)
                                                    .collect(Collectors.toSet());
    Assert.assertEquals(
        new HashSet<>(segments),
        segmentRetriever.findPublishedSegments(searchSegmentIds)
    );

    EasyMock.verify(taskActionClient);
  }

  @Test
  public void testSegmentsForMultipleDatasourcesThrowsException()
  {
    DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> segmentRetriever.findPublishedSegments(
            ImmutableSet.of(
                SegmentId.of(TestDataSource.WIKI, Intervals.ETERNITY, "v1", 0),
                SegmentId.of(TestDataSource.KOALA, Intervals.ETERNITY, "v1", 0)
            )
        )
    );
    Assert.assertEquals(
        "Published segment IDs to find cannot belong to multiple datasources[wiki, koala].",
        exception.getMessage()
    );
  }
}
