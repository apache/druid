/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class DynamicUpstreamReporterTest
{
  private static final String DEFAULT_UPSTREAM = "upstream_service";
  final Object memoryBarrier = new Object();
  DynamicUpstreamReporter reporter = null;
  final TaskMaster taskMaster = EasyMock.createStrictMock(TaskMaster.class);
  final HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);
  @Mock(type = MockType.STRICT)
  ServiceDiscovery<Void> serviceDiscovery;
  @Mock(type = MockType.STRICT)
  ServiceInstance<Void> service;
  @Mock(type = MockType.STRICT)
  TaskRunnerReporter runner;
  final TaskStatus status = TaskStatus.success("task_id");

  final
  @Before
  public void setUp() throws Exception
  {
    EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
            .andReturn(ImmutableList.of(service))
            .anyTimes();
    EasyMock.expect(runner.reportStatus(EasyMock.eq(status))).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>of(runner)).anyTimes();
    EasyMock.replay(serviceDiscovery, taskMaster, httpClient, runner);
    reporter = new DynamicUpstreamReporter(
        taskMaster,
        httpClient,
        serviceDiscovery,
        DEFAULT_UPSTREAM
    );
  }

  @Test
  public void testReportStatusSimple() throws Exception
  {
    Assert.assertTrue(reporter.reportStatus(status));
  }

  @Test
  public void testReportStatusMissingRunner() throws Exception
  {
    synchronized (memoryBarrier) {
      EasyMock.reset(taskMaster);
      EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).once();
      EasyMock.replay(taskMaster);
    }
    testPost();
  }

  @Test
  public void testReportStatusSillyRunner() throws Exception
  {
    synchronized (memoryBarrier) {
      final TaskRunner notReporter = EasyMock.createStrictMock(TaskRunner.class);
      EasyMock.reset(taskMaster);
      EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(notReporter)).once();
      EasyMock.replay(taskMaster);
    }
    testPost();
  }

  @Test
  public void testReportStatusNullManager() throws Exception
  {
    reporter = new DynamicUpstreamReporter(
        null,
        httpClient,
        serviceDiscovery,
        DEFAULT_UPSTREAM
    );
    testPost();
  }


  private void testPost()
  {
    synchronized (memoryBarrier) {
      EasyMock.reset(service);
      EasyMock.expect(service.getAddress()).andReturn("localhost").once();
      EasyMock.expect(service.getPort()).andReturn(0).once();
      EasyMock.replay(service);

      final SettableFuture<StatusResponseHolder> future = SettableFuture.create();
      future.set(new StatusResponseHolder(HttpResponseStatus.ACCEPTED, new StringBuilder()));

      EasyMock.reset(httpClient);
      EasyMock.expect(httpClient.go(
          EasyMock.anyObject(Request.class),
          EasyMock.anyObject(StatusResponseHandler.class),
          EasyMock.eq(Duration.parse("PT60s"))
      )).andReturn(future).once();
      EasyMock.replay(httpClient);
    }
    Assert.assertTrue(reporter.reportStatus(status));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(serviceDiscovery, taskMaster, httpClient, runner);
  }
}

abstract class TaskRunnerReporter implements TaskRunner, TaskStatusReporter
{
  // Nothing
}

