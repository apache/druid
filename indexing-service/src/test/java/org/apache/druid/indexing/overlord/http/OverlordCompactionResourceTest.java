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

package org.apache.druid.indexing.overlord.http;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.compaction.CompactionProgressResponse;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OverlordCompactionResourceTest
{
  private CompactionScheduler scheduler;
  private OverlordCompactionResource compactionResource;
  private CoordinatorClient coordinatorClient;
  private SupervisorResource supervisorResource;

  @Before
  public void setUp()
  {
    scheduler = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(scheduler.isEnabled()).andReturn(true).anyTimes();

    coordinatorClient = EasyMock.createStrictMock(CoordinatorClient.class);
    supervisorResource = EasyMock.createStrictMock(SupervisorResource.class);

    compactionResource = new OverlordCompactionResource(
        scheduler,
        coordinatorClient,
        supervisorResource
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(scheduler, coordinatorClient);
  }

  private void replayAll()
  {
    EasyMock.replay(scheduler, coordinatorClient);
  }

  @Test
  public void testGetCompactionSnapshotWithEmptyDatasource()
  {
    final Map<String, AutoCompactionSnapshot> allSnapshots = ImmutableMap.of(
        TestDataSource.WIKI,
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build()
    );

    EasyMock.expect(scheduler.getAllCompactionSnapshots())
            .andReturn(allSnapshots).once();
    replayAll();

    final Response response = compactionResource.getCompactionSnapshots("");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        new CompactionStatusResponse(allSnapshots.values()),
        response.getEntity()
    );
  }

  @Test
  public void testGetCompactionSnapshotWithNullDatasource()
  {
    final Map<String, AutoCompactionSnapshot> allSnapshots = ImmutableMap.of(
        TestDataSource.WIKI,
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build()
    );

    EasyMock.expect(scheduler.getAllCompactionSnapshots())
            .andReturn(allSnapshots).once();
    replayAll();

    final Response response = compactionResource.getCompactionSnapshots(null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        new CompactionStatusResponse(allSnapshots.values()),
        response.getEntity()
    );
  }

  @Test
  public void testGetCompactionSnapshotWithValidDatasource()
  {
    final AutoCompactionSnapshot snapshot = AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();

    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.WIKI))
            .andReturn(snapshot).once();
    replayAll();

    final Response response = compactionResource.getCompactionSnapshots(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        new CompactionStatusResponse(Collections.singleton(snapshot)),
        response.getEntity()
    );
  }

  @Test
  public void testGetCompactionSnapshotWithInvalidDatasource()
  {
    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.KOALA))
            .andReturn(null).once();
    replayAll();

    final Response response = compactionResource.getCompactionSnapshots(TestDataSource.KOALA);
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testGetProgressForValidDatasource()
  {
    final AutoCompactionSnapshot.Builder snapshotBuilder
        = AutoCompactionSnapshot.builder(TestDataSource.WIKI);
    snapshotBuilder.incrementWaitingStats(CompactionStatistics.create(100L, 10L, 1L));
    final AutoCompactionSnapshot snapshot = snapshotBuilder.build();

    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.WIKI))
            .andReturn(snapshot).once();
    replayAll();

    final Response response = compactionResource.getCompactionProgress(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionProgressResponse(100L), response.getEntity());
  }

  @Test
  public void testGetProgressForNullDatasourceReturnsBadRequest()
  {
    replayAll();

    final Response response = compactionResource.getCompactionProgress(null);
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

    final Object responseEntity = response.getEntity();
    Assert.assertTrue(responseEntity instanceof ErrorResponse);

    MatcherAssert.assertThat(
        ((ErrorResponse) responseEntity).getUnderlyingException(),
        DruidExceptionMatcher.invalidInput().expectMessageIs("No DataSource specified")
    );
  }

  @Test
  public void testGetProgressForInvalidDatasourceReturnsNotFound()
  {
    EasyMock.expect(scheduler.getCompactionSnapshot(TestDataSource.KOALA))
            .andReturn(null).once();
    replayAll();

    final Response response = compactionResource.getCompactionProgress(TestDataSource.KOALA);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

    final Object responseEntity = response.getEntity();
    Assert.assertTrue(responseEntity instanceof ErrorResponse);

    MatcherAssert.assertThat(
        ((ErrorResponse) responseEntity).getUnderlyingException(),
        DruidExceptionMatcher.notFound().expectMessageIs("Unknown DataSource")
    );
  }

  @Test
  public void testGetProgress_redirectsToCoordinator_ifSchedulerIsDisabled()
  {
    scheduler = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(scheduler.isEnabled()).andReturn(false).once();

    EasyMock.expect(coordinatorClient.getBytesAwaitingCompaction(TestDataSource.WIKI))
            .andReturn(Futures.immediateFuture(new CompactionProgressResponse(100L)));
    replayAll();

    final Response response =
        new OverlordCompactionResource(scheduler, coordinatorClient, supervisorResource)
            .getCompactionProgress(TestDataSource.WIKI);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionProgressResponse(100L), response.getEntity());
  }

  @Test
  public void testGetSnapshot_redirectsToCoordinator_ifSchedulerIsDisabled()
  {
    scheduler = EasyMock.createStrictMock(CompactionScheduler.class);
    EasyMock.expect(scheduler.isEnabled()).andReturn(false).once();

    final AutoCompactionSnapshot snapshot =
        AutoCompactionSnapshot.builder(TestDataSource.WIKI).build();
    EasyMock.expect(coordinatorClient.getCompactionSnapshots(TestDataSource.WIKI))
            .andReturn(Futures.immediateFuture(new CompactionStatusResponse(List.of(snapshot))));
    replayAll();

    final Response response =
        new OverlordCompactionResource(scheduler, coordinatorClient, supervisorResource)
            .getCompactionSnapshots(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionStatusResponse(List.of(snapshot)), response.getEntity());
  }
}
