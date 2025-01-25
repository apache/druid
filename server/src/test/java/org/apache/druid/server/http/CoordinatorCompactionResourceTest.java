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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionStatistics;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;

public class CoordinatorCompactionResourceTest
{
  private DruidCoordinator mock;
  private OverlordClient overlordClient;
  private final String dataSourceName = "datasource_1";
  private final AutoCompactionSnapshot expectedSnapshot = new AutoCompactionSnapshot(
      dataSourceName,
      AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
      null,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1
  );

  @Before
  public void setUp()
  {
    mock = EasyMock.createStrictMock(DruidCoordinator.class);
    overlordClient = new NoopOverlordClient()
    {
      @Override
      public ListenableFuture<Boolean> isCompactionSupervisorEnabled()
      {
        return Futures.immediateFuture(false);
      }
    };
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mock);
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithEmptyQueryParameter()
  {
    Map<String, AutoCompactionSnapshot> expected = ImmutableMap.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
        .getCompactionSnapshotForDataSource("");
    Assert.assertEquals(new CompactionStatusResponse(expected.values()), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithNullQueryParameter()
  {
    String dataSourceName = "datasource_1";
    Map<String, AutoCompactionSnapshot> expected = ImmutableMap.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
        .getCompactionSnapshotForDataSource(null);
    Assert.assertEquals(new CompactionStatusResponse(expected.values()), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithValidQueryParameter()
  {
    String dataSourceName = "datasource_1";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName))
            .andReturn(expectedSnapshot).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
        .getCompactionSnapshotForDataSource(dataSourceName);
    Assert.assertEquals(
        new CompactionStatusResponse(Collections.singletonList(expectedSnapshot)),
        response.getEntity()
    );
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithInvalidQueryParameter()
  {
    String dataSourceName = "invalid_datasource";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName))
            .andReturn(null).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
        .getCompactionSnapshotForDataSource(dataSourceName);
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testGetProgressForNullDatasourceReturnsBadRequest()
  {
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
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
  public void testGetSnapshotRedirectsToOverlordWhenSupervisorIsEnabled()
  {
    EasyMock.replay(mock);

    AutoCompactionSnapshot.Builder snapshotBuilder = AutoCompactionSnapshot.builder(dataSourceName);
    snapshotBuilder.incrementCompactedStats(CompactionStatistics.create(100L, 10L, 1L));
    final AutoCompactionSnapshot snapshotFromOverlord = snapshotBuilder.build();

    overlordClient = new NoopOverlordClient() {
      @Override
      public ListenableFuture<Boolean> isCompactionSupervisorEnabled()
      {
        return Futures.immediateFuture(true);
      }

      @Override
      public ListenableFuture<CompactionStatusResponse> getCompactionSnapshots(@Nullable String dataSource)
      {
        return Futures.immediateFuture(
            new CompactionStatusResponse(Collections.singletonList(snapshotFromOverlord))
        );
      }
    };

    final Response response = new CoordinatorCompactionResource(mock, overlordClient)
        .getCompactionSnapshotForDataSource(dataSourceName);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        new CompactionStatusResponse(Collections.singletonList(snapshotFromOverlord)),
        response.getEntity()
    );
  }
}
