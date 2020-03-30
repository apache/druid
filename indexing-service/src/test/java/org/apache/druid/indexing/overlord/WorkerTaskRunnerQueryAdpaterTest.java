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

package org.apache.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;


public class WorkerTaskRunnerQueryAdpaterTest
{
  private WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter;
  private HttpClient httpClient;
  private WorkerTaskRunner workerTaskRunner;
  private TaskMaster taskMaster;

  @Before
  public void setup()
  {
    httpClient = EasyMock.createNiceMock(HttpClient.class);
    workerTaskRunner = EasyMock.createMock(WorkerTaskRunner.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);

    workerTaskRunnerQueryAdapter = new WorkerTaskRunnerQueryAdapter(taskMaster, httpClient);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(workerTaskRunner)
    ).once();

    EasyMock.expect(workerTaskRunner.getWorkers()).andReturn(
        ImmutableList.of(
            new ImmutableWorkerInfo(
                new Worker(
                    "http", "worker-host1", "192.0.0.1", 10, "v1", WorkerConfig.DEFAULT_CATEGORY
                ),
                2,
                ImmutableSet.of("grp1", "grp2"),
                ImmutableSet.of("task1", "task2"),
                DateTimes.of("2015-01-01T01:01:01Z")
            ),
            new ImmutableWorkerInfo(
                new Worker(
                    "https", "worker-host2", "192.0.0.2", 4, "v1", WorkerConfig.DEFAULT_CATEGORY
                ),
                1,
                ImmutableSet.of("grp1"),
                ImmutableSet.of("task1"),
                DateTimes.of("2015-01-01T01:01:01Z")
            )
        )
    ).once();
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(workerTaskRunner, taskMaster, httpClient);
  }

  @Test
  public void testDisableWorker() throws Exception
  {
    final URL workerUrl = new URL("http://worker-host1/druid/worker/v1/disable");
    final String workerResponse = "{\"worker-host1\":\"disabled\"}";
    Capture<Request> capturedRequest = getHttpClientRequestCapture(HttpResponseStatus.OK, workerResponse);

    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    workerTaskRunnerQueryAdapter.disableWorker("worker-host1");

    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(workerUrl, capturedRequest.getValue().getUrl());
  }

  @Test
  public void testDisableWorkerWhenWorkerRaisesError() throws Exception
  {
    final URL workerUrl = new URL("http://worker-host1/druid/worker/v1/disable");
    Capture<Request> capturedRequest = getHttpClientRequestCapture(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");

    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    try {
      workerTaskRunnerQueryAdapter.disableWorker("worker-host1");
      Assert.fail("Should raise RE exception!");
    }
    catch (RE re) {
    }

    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(workerUrl, capturedRequest.getValue().getUrl());
  }

  @Test(expected = RE.class)
  public void testDisableWorkerWhenWorkerNotExists()
  {
    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    workerTaskRunnerQueryAdapter.disableWorker("not-existing-worker");
  }

  @Test
  public void testEnableWorker() throws Exception
  {
    final URL workerUrl = new URL("https://worker-host2/druid/worker/v1/enable");
    final String workerResponse = "{\"worker-host2\":\"enabled\"}";
    Capture<Request> capturedRequest = getHttpClientRequestCapture(HttpResponseStatus.OK, workerResponse);

    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    workerTaskRunnerQueryAdapter.enableWorker("worker-host2");

    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(workerUrl, capturedRequest.getValue().getUrl());
  }

  @Test
  public void testEnableWorkerWhenWorkerRaisesError() throws Exception
  {
    final URL workerUrl = new URL("https://worker-host2/druid/worker/v1/enable");
    Capture<Request> capturedRequest = getHttpClientRequestCapture(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");

    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    try {
      workerTaskRunnerQueryAdapter.enableWorker("worker-host2");
      Assert.fail("Should raise RE exception!");
    }
    catch (RE re) {
    }

    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(workerUrl, capturedRequest.getValue().getUrl());
  }

  @Test(expected = RE.class)
  public void testEnableWorkerWhenWorkerNotExists()
  {
    EasyMock.replay(workerTaskRunner, taskMaster, httpClient);

    workerTaskRunnerQueryAdapter.enableWorker("not-existing-worker");
  }

  private Capture<Request> getHttpClientRequestCapture(HttpResponseStatus httpStatus, String responseContent)
  {
    SettableFuture<StatusResponseHolder> futureResult = SettableFuture.create();
    futureResult.set(
        new StatusResponseHolder(httpStatus, new StringBuilder(responseContent))
    );
    Capture<Request> capturedRequest = EasyMock.newCapture();
    EasyMock.expect(
        httpClient.go(
            EasyMock.capture(capturedRequest),
            EasyMock.<HttpResponseHandler>anyObject()
        )
    )
            .andReturn(futureResult)
            .once();

    return capturedRequest;
  }
}
