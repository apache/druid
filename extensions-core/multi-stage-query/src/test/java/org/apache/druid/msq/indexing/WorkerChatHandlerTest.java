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

package org.apache.druid.msq.indexing;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.indexing.client.WorkerChatHandler;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.InputStream;

public class WorkerChatHandlerTest
{
  private static final StageId TEST_STAGE = new StageId("123", 0);
  private static final String DATASOURCE = "foo";

  @Mock
  private HttpServletRequest req;

  private AuthorizerMapper authorizerMapper;
  private AutoCloseable mocks;

  private final TestWorker worker = new TestWorker();

  @Before
  public void setUp()
  {
    authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
    mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult("druid", "druid", null, null));
  }

  @Test
  public void testFetchSnapshot()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(worker, authorizerMapper, DATASOURCE);
    Assert.assertEquals(
        ClusterByStatisticsSnapshot.empty(),
        chatHandler.httpFetchKeyStatistics(TEST_STAGE.getQueryId(), TEST_STAGE.getStageNumber(), null, req)
                   .getEntity()
    );
  }

  @Test
  public void testFetchSnapshot404()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(worker, authorizerMapper, DATASOURCE);
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        chatHandler.httpFetchKeyStatistics("123", 2, null, req)
                   .getStatus()
    );
  }

  @Test
  public void testFetchSnapshotWithTimeChunk()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(worker, authorizerMapper, DATASOURCE);
    Assert.assertEquals(
        ClusterByStatisticsSnapshot.empty(),
        chatHandler.httpFetchKeyStatisticsWithSnapshot(TEST_STAGE.getQueryId(), TEST_STAGE.getStageNumber(), 1, null, req)
                   .getEntity()
    );
  }

  @Test
  public void testFetchSnapshotWithTimeChunk404()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(worker, authorizerMapper, DATASOURCE);
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        chatHandler.httpFetchKeyStatisticsWithSnapshot("123", 2, 1, null, req)
                   .getStatus()
    );
  }


  private static class TestWorker implements Worker
  {
    @Override
    public String id()
    {
      return TEST_STAGE.getQueryId() + "task";
    }

    @Override
    public void run()
    {

    }

    @Override
    public void stop()
    {

    }

    @Override
    public void controllerFailed()
    {

    }

    @Override
    public void awaitStop()
    {

    }

    @Override
    public void postWorkOrder(WorkOrder workOrder)
    {

    }

    @Override
    public ClusterByStatisticsSnapshot fetchStatisticsSnapshot(StageId stageId)
    {
      if (TEST_STAGE.equals(stageId)) {
        return ClusterByStatisticsSnapshot.empty();
      } else {
        throw new ISE("stage not found %s", stageId);
      }
    }

    @Override
    public ClusterByStatisticsSnapshot fetchStatisticsSnapshotForTimeChunk(StageId stageId, long timeChunk)
    {
      if (TEST_STAGE.equals(stageId)) {
        return ClusterByStatisticsSnapshot.empty();
      } else {
        throw new ISE("stage not found %s", stageId);
      }
    }

    @Override
    public boolean postResultPartitionBoundaries(
        StageId stageId,
        ClusterByPartitions stagePartitionBoundaries
    )
    {
      return false;
    }

    @Nullable
    @Override
    public ListenableFuture<InputStream> readStageOutput(StageId stageId, int partitionNumber, long offset)
    {
      return null;
    }

    @Override
    public CounterSnapshotsTree getCounters()
    {
      return null;
    }

    @Override
    public void postCleanupStage(StageId stageId)
    {

    }

    @Override
    public void postFinish()
    {

    }
  }

  @After
  public void tearDown()
  {
    try {
      mocks.close();
    }
    catch (Exception ignored) {
      // ignore tear down exceptions
    }
  }
}
