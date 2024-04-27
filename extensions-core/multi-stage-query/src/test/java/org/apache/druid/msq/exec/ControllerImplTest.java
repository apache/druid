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

package org.apache.druid.msq.exec;

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.doReturn;

public class ControllerImplTest
{

  @Mock
  private StageDefinition stageDefinition;
  @Mock
  private ClusterBy clusterBy;
  private AutoCloseable mocks;


  @Before
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);
    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();
    doReturn(clusterBy).when(stageDefinition).getClusterBy();

  }

  @Test
  public void test_performSegmentPublish_ok() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null);

    final TaskActionClient taskActionClient = EasyMock.mock(TaskActionClient.class);
    EasyMock.expect(taskActionClient.submit(action)).andReturn(SegmentPublishResult.ok(Collections.emptySet()));
    EasyMock.replay(taskActionClient);

    // All OK.
    ControllerImpl.performSegmentPublish(taskActionClient, action);
  }

  @Test
  public void test_performSegmentPublish_publishFail() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null);

    final TaskActionClient taskActionClient = EasyMock.mock(TaskActionClient.class);
    EasyMock.expect(taskActionClient.submit(action)).andReturn(SegmentPublishResult.fail("oops"));
    EasyMock.replay(taskActionClient);

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    Assert.assertEquals(InsertLockPreemptedFault.instance(), e.getFault());
  }

  @Test
  public void test_performSegmentPublish_publishException() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null);

    final TaskActionClient taskActionClient = EasyMock.mock(TaskActionClient.class);
    EasyMock.expect(taskActionClient.submit(action)).andThrow(new ISE("oops"));
    EasyMock.replay(taskActionClient);

    final ISE e = Assert.assertThrows(
        ISE.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    Assert.assertEquals("oops", e.getMessage());
  }

  @Test
  public void test_performSegmentPublish_publishLockPreemptedException() throws IOException
  {
    final SegmentTransactionalInsertAction action =
        SegmentTransactionalInsertAction.appendAction(Collections.emptySet(), null, null, null);

    final TaskActionClient taskActionClient = EasyMock.mock(TaskActionClient.class);
    EasyMock.expect(taskActionClient.submit(action)).andThrow(new ISE("are not covered by locks"));
    EasyMock.replay(taskActionClient);

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> ControllerImpl.performSegmentPublish(taskActionClient, action)
    );

    Assert.assertEquals(InsertLockPreemptedFault.instance(), e.getFault());
  }


  @Test
  public void test_belowThresholds_ShouldBeParallel()
  {
    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count below threshold
    doReturn(1).when(stageDefinition).getMaxWorkerCount();

    Assert.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );
  }


  @Test
  public void test_noClusterByColumns_shouldBeParallel()
  {

    // Cluster by bucket count 0
    doReturn(ClusterBy.none()).when(stageDefinition).getClusterBy();

    // Worker count above threshold
    doReturn((int) Limits.MAX_WORKERS_FOR_PARALLEL_MERGE + 1).when(stageDefinition).getMaxWorkerCount();

    Assert.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );

  }

  @Test
  public void test_numWorkersAboveThreshold_shouldBeSequential()
  {
    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count above threshold
    doReturn((int) Limits.MAX_WORKERS_FOR_PARALLEL_MERGE + 1).when(stageDefinition).getMaxWorkerCount();

    Assert.assertEquals(
        ClusterStatisticsMergeMode.SEQUENTIAL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(
            stageDefinition,
            ClusterStatisticsMergeMode.AUTO
        )
    );

  }

  @Test
  public void test_mode_should_not_change()
  {

    Assert.assertEquals(
        ClusterStatisticsMergeMode.SEQUENTIAL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(null, ClusterStatisticsMergeMode.SEQUENTIAL)
    );
    Assert.assertEquals(
        ClusterStatisticsMergeMode.PARALLEL,
        ControllerImpl.finalizeClusterStatisticsMergeMode(null, ClusterStatisticsMergeMode.PARALLEL)
    );
  }


  @After
  public void tearDown() throws Exception
  {
    mocks.close();
  }
}
