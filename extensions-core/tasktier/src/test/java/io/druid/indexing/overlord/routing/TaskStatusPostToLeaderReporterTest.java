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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.ISE;
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
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;

public class TaskStatusPostToLeaderReporterTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private static final String DEFAULT_UPSTREAM = "upstream_service";
  final Object memoryBarrier = new Object();
  TaskStatusPostToLeaderReporter reporter = null;
  final TaskMaster taskMaster = EasyMock.createStrictMock(TaskMaster.class);
  final HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);
  final ServiceDiscovery<Void> serviceDiscovery = EasyMock.createStrictMock(ServiceDiscovery.class);
  final ServiceInstance<Void> service = EasyMock.createStrictMock(ServiceInstance.class);
  final TaskRunnerReporter runner = EasyMock.createStrictMock(TaskRunnerReporter.class);
  final TaskStatus status = TaskStatus.success("task_id");

  final
  @Before
  public void setUp() throws Exception
  {
    EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
            .andReturn(ImmutableList.of(service))
            .once();
    EasyMock.expect(service.getAddress()).andReturn("localhost").once();
    EasyMock.expect(service.getPort()).andReturn(0).once();
    EasyMock.expect(runner.reportStatus(EasyMock.eq(status))).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>of(runner)).anyTimes();
    EasyMock.replay(serviceDiscovery, taskMaster, httpClient, runner, service);
    reporter = new TaskStatusPostToLeaderReporter(
        httpClient,
        serviceDiscovery,
        DEFAULT_UPSTREAM
    );
  }

  @Test
  public void testReportStatus() throws Exception
  {
    synchronized (memoryBarrier) {
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


  @Test
  public void testReportStatusFailed() throws Exception
  {
    synchronized (memoryBarrier) {
      final SettableFuture<StatusResponseHolder> future = SettableFuture.create();
      future.set(new StatusResponseHolder(HttpResponseStatus.SERVICE_UNAVAILABLE, new StringBuilder()));
      EasyMock.reset(httpClient);
      EasyMock.expect(httpClient.go(
          EasyMock.anyObject(Request.class),
          EasyMock.anyObject(StatusResponseHandler.class),
          EasyMock.eq(Duration.parse("PT60s"))
      )).andReturn(future).once();
      EasyMock.replay(httpClient);
    }
    Assert.assertFalse(reporter.reportStatus(status));
  }


  @Test
  public void testReportStatusException() throws Exception
  {
    synchronized (memoryBarrier) {
      final SettableFuture<StatusResponseHolder> future = SettableFuture.create();
      future.set(new StatusResponseHolder(HttpResponseStatus.BAD_GATEWAY, new StringBuilder()));
      EasyMock.reset(httpClient);
      EasyMock.expect(httpClient.go(
          EasyMock.anyObject(Request.class),
          EasyMock.anyObject(StatusResponseHandler.class),
          EasyMock.eq(Duration.parse("PT60s"))
      )).andReturn(future).once();
      EasyMock.replay(httpClient);
    }
    expectedException.expect(new BaseMatcher<ISE>()
    {
      @Override
      public boolean matches(Object o)
      {
        if (o instanceof ISE) {
          final ISE ise = (ISE) o;
          return ise.getMessage().startsWith("Unknown response [");
        }
        return false;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }

  @Test
  public void testReportStatusExecutionException() throws Exception
  {
    final RuntimeException ex = new RuntimeException("test exception");
    synchronized (memoryBarrier) {
      final SettableFuture<StatusResponseHolder> future = SettableFuture.create();
      future.setException(ex);
      EasyMock.reset(httpClient);
      EasyMock.expect(httpClient.go(
          EasyMock.anyObject(Request.class),
          EasyMock.anyObject(StatusResponseHandler.class),
          EasyMock.eq(Duration.parse("PT60s"))
      )).andReturn(future).once();
      EasyMock.replay(httpClient);
    }
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return (o instanceof ExecutionException) && ((ExecutionException) o).getCause() == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }

  @Test
  public void testReportStatusExecutionInterrupted() throws Exception
  {
    final InterruptedException ex = new InterruptedException("test exception");
    final ListenableFuture<StatusResponseHolder> future = EasyMock.createStrictMock(ListenableFuture.class);
    synchronized (memoryBarrier) {
      EasyMock.expect(future.get()).andThrow(ex).once();
      EasyMock.replay(future);
      EasyMock.reset(httpClient);
      EasyMock.expect(httpClient.go(
          EasyMock.anyObject(Request.class),
          EasyMock.anyObject(StatusResponseHandler.class),
          EasyMock.eq(Duration.parse("PT60s"))
      )).andReturn(future).once();
      EasyMock.replay(httpClient);
    }
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    try {
      reporter.reportStatus(status);
    }
    finally {
      EasyMock.verify(future);
    }
  }


  @Test
  public void testReportStatusMalformedURL() throws Exception
  {
    synchronized (memoryBarrier) {
      EasyMock.reset(service);
      EasyMock.expect(service.getAddress()).andReturn("localhost").once();
      EasyMock.expect(service.getPort()).andReturn(-2).once();
      EasyMock.replay(service);
    }
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof MalformedURLException;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }

  @Test
  public void testReportStatusNoOverlord() throws Exception
  {
    synchronized (memoryBarrier) {
      EasyMock.reset(serviceDiscovery);
      EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
              .andReturn(ImmutableList.<ServiceInstance<Void>>of())
              .once();
      EasyMock.replay(serviceDiscovery);
      EasyMock.reset(service);
      EasyMock.replay(service);
    }
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof ISE && ((ISE) o).getMessage()
                                            .equals(String.format(
                                                "No overlords found for service [%s]",
                                                DEFAULT_UPSTREAM
                                            ));
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }


  @Test
  public void testReportStatusQueryInterrupted() throws Exception
  {
    final InterruptedException ex = new InterruptedException("test exception");
    synchronized (memoryBarrier) {
      EasyMock.reset(serviceDiscovery);
      EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
              .andThrow(ex).once();
      EasyMock.replay(serviceDiscovery);
      EasyMock.reset(service);
      EasyMock.replay(service);
    }
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }

  @Test
  public void testReportStatusQueryRuntimeException() throws Exception
  {
    final RuntimeException ex = new RuntimeException("test exception");
    synchronized (memoryBarrier) {
      EasyMock.reset(serviceDiscovery);
      EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
              .andThrow(ex).once();
      EasyMock.replay(serviceDiscovery);
      EasyMock.reset(service);
      EasyMock.replay(service);
    }
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }


  @Test
  public void testReportStatusQueryException() throws Exception
  {
    final Exception ex = new Exception("test exception");
    synchronized (memoryBarrier) {
      EasyMock.reset(serviceDiscovery);
      EasyMock.expect(serviceDiscovery.queryForInstances(EasyMock.eq(DEFAULT_UPSTREAM)))
              .andThrow(ex).once();
      EasyMock.replay(serviceDiscovery);
      EasyMock.reset(service);
      EasyMock.replay(service);
    }
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    reporter.reportStatus(status);
  }

  @After
  public void tearDown()
  {
    // Clear interrupt
    Thread.interrupted();
    synchronized (memoryBarrier) {
      EasyMock.verify(serviceDiscovery, taskMaster, httpClient, runner, service);
    }
  }
}
