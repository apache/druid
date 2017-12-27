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

package io.druid.indexing.overlord;

import com.google.common.collect.ImmutableList;
import io.druid.indexing.common.task.NoopTask;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IndexerMetadataStorageAdapterTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private TaskStorageQueryAdapter taskStorageQueryAdapter;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;

  @Before
  public void setup()
  {
    indexerMetadataStorageCoordinator = EasyMock.strictMock(IndexerMetadataStorageCoordinator.class);
    taskStorageQueryAdapter = EasyMock.strictMock(TaskStorageQueryAdapter.class);
    indexerMetadataStorageAdapter = new IndexerMetadataStorageAdapter(
        taskStorageQueryAdapter,
        indexerMetadataStorageCoordinator
    );
  }

  @Test
  public void testDeletePendingSegments()
  {
    EasyMock.expect(taskStorageQueryAdapter.getActiveTasks())
            .andReturn(ImmutableList.of(NoopTask.create("id1", 0), NoopTask.create("id2", 0)));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(EasyMock.eq("id1")))
            .andReturn(DateTimes.of("2017-12-01"));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(EasyMock.eq("id2")))
            .andReturn(DateTimes.of("2017-12-02"));

    final Interval deleteInterval = Intervals.of("2017-01-01/2017-12-01");
    EasyMock
        .expect(
            indexerMetadataStorageCoordinator.deletePendingSegments(EasyMock.anyString(), EasyMock.eq(deleteInterval))
        )
        .andReturn(10);
    EasyMock.replay(taskStorageQueryAdapter, indexerMetadataStorageCoordinator);

    Assert.assertEquals(10, indexerMetadataStorageAdapter.deletePendingSegments("dataSource", deleteInterval));
  }

  @Test
  public void testDeletePendingSegmentsOfRunningTasks()
  {
    EasyMock.expect(taskStorageQueryAdapter.getActiveTasks())
            .andReturn(ImmutableList.of(NoopTask.create("id1", 0), NoopTask.create("id2", 0)));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(EasyMock.eq("id1")))
            .andReturn(DateTimes.of("2017-11-01"));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(EasyMock.eq("id2")))
            .andReturn(DateTimes.of("2017-12-02"));

    final Interval deleteInterval = Intervals.of("2017-01-01/2017-12-01");
    EasyMock
        .expect(
            indexerMetadataStorageCoordinator.deletePendingSegments(EasyMock.anyString(), EasyMock.eq(deleteInterval))
        )
        .andReturn(10);
    EasyMock.replay(taskStorageQueryAdapter, indexerMetadataStorageCoordinator);

    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage("Cannot delete pendingSegments because there is at least one active task created");
    indexerMetadataStorageAdapter.deletePendingSegments("dataSource", deleteInterval);
  }
}
