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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.indexing.client.WorkerChatHandler;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
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
import java.util.HashMap;

public class WorkerChatHandlerTest
{
  private static final StageId TEST_STAGE = new StageId("123", 0);
  @Mock
  private HttpServletRequest req;

  private TaskToolbox toolbox;
  private AutoCloseable mocks;

  private final TestWorker worker = new TestWorker();

  @Before
  public void setUp()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    IndexIO indexIO = new IndexIO(mapper, ColumnConfig.DEFAULT);
    IndexMergerV9 indexMerger = new IndexMergerV9(
        mapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
           .thenReturn(new AuthenticationResult("druid", "druid", null, null));
    TaskToolbox.Builder builder = new TaskToolbox.Builder();
    toolbox = builder.authorizerMapper(CalciteTests.TEST_AUTHORIZER_MAPPER)
                     .indexIO(indexIO)
                     .indexMergerV9(indexMerger)
                     .taskReportFileWriter(new NoopTestTaskReportFileWriter())
                     .build();
  }

  @Test
  public void testFetchSnapshot()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    Assert.assertEquals(
        ClusterByStatisticsSnapshot.empty(),
        chatHandler.httpFetchKeyStatistics(TEST_STAGE.getQueryId(), TEST_STAGE.getStageNumber(), req)
                   .getEntity()
    );
  }

  @Test
  public void testFetchSnapshot404()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        chatHandler.httpFetchKeyStatistics("123", 2, req)
                   .getStatus()
    );
  }

  @Test
  public void testFetchSnapshotWithTimeChunk()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    Assert.assertEquals(
        ClusterByStatisticsSnapshot.empty(),
        chatHandler.httpFetchKeyStatisticsWithSnapshot(TEST_STAGE.getQueryId(), TEST_STAGE.getStageNumber(), 1, req)
                   .getEntity()
    );
  }

  @Test
  public void testFetchSnapshotWithTimeChunk404()
  {
    WorkerChatHandler chatHandler = new WorkerChatHandler(toolbox, worker);
    Assert.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(),
        chatHandler.httpFetchKeyStatisticsWithSnapshot("123", 2, 1, req)
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
    public MSQWorkerTask task()
    {
      return new MSQWorkerTask("controller", "ds", 1, new HashMap<>(), 0);
    }

    @Override
    public TaskStatus run()
    {
      return null;
    }

    @Override
    public void stopGracefully()
    {

    }

    @Override
    public void controllerFailed()
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
        ClusterByPartitions stagePartitionBoundaries,
        String queryId,
        int stageNumber
    )
    {
      return false;
    }

    @Nullable
    @Override
    public InputStream readChannel(String queryId, int stageNumber, int partitionNumber, long offset)
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
