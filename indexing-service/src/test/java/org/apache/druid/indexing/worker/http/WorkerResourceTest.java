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

package org.apache.druid.indexing.worker.http;

import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerTaskManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 */
public class WorkerResourceTest
{
  private Worker worker;
  private WorkerTaskManager workerTaskManager;
  private WorkerResource workerResource;

  @Before
  public void setUp()
  {
    worker = new Worker(
        "http",
        "host",
        "ip",
        3,
        "v1",
        WorkerConfig.DEFAULT_CATEGORY
    );
    workerTaskManager = EasyMock.createMock(WorkerTaskManager.class);
    workerResource = new WorkerResource(
        worker,
        EasyMock.createNiceMock(TaskRunner.class),
        workerTaskManager
    );
  }

  @Test
  public void testDoDisable()
  {
    workerTaskManager.workerDisabled();
    EasyMock.expectLastCall();
    EasyMock.replay(workerTaskManager);

    Response res = workerResource.doDisable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    EasyMock.verify(workerTaskManager);
  }

  @Test
  public void testDoEnable()
  {
    workerTaskManager.workerEnabled();
    EasyMock.expectLastCall();
    EasyMock.replay(workerTaskManager);

    Response res = workerResource.doEnable();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    EasyMock.verify(workerTaskManager);
  }

  @Test
  public void testIsEnabled()
  {
    EasyMock.expect(workerTaskManager.isWorkerEnabled()).andReturn(true);
    EasyMock.replay(workerTaskManager);

    Response res = workerResource.isEnabled();
    Assert.assertEquals(Response.Status.OK.getStatusCode(), res.getStatus());

    EasyMock.verify(workerTaskManager);
  }
}
