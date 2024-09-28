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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.compaction.CompactionProgressResponse;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CompactionSupervisorConfig;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;

public class OverlordCompactionResourceTest
{
  private static final CompactionSupervisorConfig SUPERVISOR_ENABLED
      = new CompactionSupervisorConfig(true, null);
  private static final CompactionSupervisorConfig SUPERVISOR_DISABLED
      = new CompactionSupervisorConfig(false, null);

  private CompactionScheduler scheduler;

  @Before
  public void setUp()
  {
    scheduler = EasyMock.createStrictMock(CompactionScheduler.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(scheduler);
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionSnapshots("");
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionSnapshots(null);
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionSnapshots(TestDataSource.WIKI);
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionSnapshots(TestDataSource.KOALA);
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionProgress(TestDataSource.WIKI);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(new CompactionProgressResponse(100L), response.getEntity());
  }

  @Test
  public void testGetProgressForNullDatasourceReturnsBadRequest()
  {
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionProgress(null);
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
    EasyMock.replay(scheduler);

    final Response response = new OverlordCompactionResource(SUPERVISOR_ENABLED, scheduler)
        .getCompactionProgress(TestDataSource.KOALA);
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());

    final Object responseEntity = response.getEntity();
    Assert.assertTrue(responseEntity instanceof ErrorResponse);

    MatcherAssert.assertThat(
        ((ErrorResponse) responseEntity).getUnderlyingException(),
        DruidExceptionMatcher.notFound().expectMessageIs("Unknown DataSource")
    );
  }

  @Test
  public void testGetProgressReturnsUnsupportedWhenSupervisorDisabled()
  {
    EasyMock.replay(scheduler);
    verifyResponseWhenSupervisorDisabled(
        new OverlordCompactionResource(SUPERVISOR_DISABLED, scheduler)
            .getCompactionProgress(TestDataSource.WIKI)
    );
  }

  @Test
  public void testGetSnapshotReturnsUnsupportedWhenSupervisorDisabled()
  {
    EasyMock.replay(scheduler);
    verifyResponseWhenSupervisorDisabled(
        new OverlordCompactionResource(SUPERVISOR_DISABLED, scheduler)
            .getCompactionSnapshots(TestDataSource.WIKI)
    );
  }

  private void verifyResponseWhenSupervisorDisabled(Response response)
  {
    Assert.assertEquals(501, response.getStatus());

    final Object responseEntity = response.getEntity();
    Assert.assertTrue(responseEntity instanceof ErrorResponse);

    MatcherAssert.assertThat(
        ((ErrorResponse) responseEntity).getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.UNSUPPORTED,
            "general"
        ).expectMessageIs(
            "Compaction Supervisors are disabled on the Overlord."
            + " Use Coordinator APIs to fetch compaction status."
        )
    );
  }
}
