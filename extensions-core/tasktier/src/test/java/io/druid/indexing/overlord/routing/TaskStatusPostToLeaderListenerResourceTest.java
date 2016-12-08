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

import io.druid.indexing.common.TaskStatus;
import io.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.net.URL;

public class TaskStatusPostToLeaderListenerResourceTest
{
  private static final TaskStatus status = TaskStatus.success("task_id");

  @Test
  public void testDoPostGood() throws Exception
  {
    final TaskStatusReporter reporter = EasyMock.createStrictMock(TaskStatusReporter.class);
    EasyMock.expect(reporter.reportStatus(EasyMock.anyObject(TaskStatus.class))).andReturn(true).once();
    EasyMock.replay(reporter);
    final TaskStatusPostToLeaderListenerResource resource = new TaskStatusPostToLeaderListenerResource(reporter);
    final Response response = resource.doPost(status);
    EasyMock.verify(reporter);
    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testDoPostBad() throws Exception
  {
    final TaskStatusReporter reporter = EasyMock.createStrictMock(TaskStatusReporter.class);
    EasyMock.expect(reporter.reportStatus(EasyMock.anyObject(TaskStatus.class))).andReturn(false).once();
    EasyMock.replay(reporter);
    final TaskStatusPostToLeaderListenerResource resource = new TaskStatusPostToLeaderListenerResource(reporter);
    final Response response = resource.doPost(status);
    EasyMock.verify(reporter);
    Assert.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  @Test
  public void testDoPostERROR() throws Exception
  {
    final RuntimeException ex = new RuntimeException("test exception");
    final TaskStatusReporter reporter = EasyMock.createStrictMock(TaskStatusReporter.class);
    EasyMock.expect(reporter.reportStatus(EasyMock.anyObject(TaskStatus.class))).andThrow(ex).once();
    EasyMock.replay(reporter);
    final TaskStatusPostToLeaderListenerResource resource = new TaskStatusPostToLeaderListenerResource(reporter);
    final Response response = resource.doPost(status);
    EasyMock.verify(reporter);
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  @Test
  public void testMakeReportUrl() throws Exception
  {
    final String host = "localhost";
    final int port = 1;
    final DruidNode node = new DruidNode("service_name", host, port);
    final URL url = TaskStatusPostToLeaderListenerResource.makeReportUrl(node);
    Assert.assertEquals(host, url.getHost());
    Assert.assertEquals(port, url.getPort());
    Assert.assertEquals("http", url.getProtocol());
    Assert.assertEquals(TaskStatusPostToLeaderListenerResource.PATH, url.getFile());
  }


  @Test
  public void testMakeReportUrlIPv6() throws Exception
  {
    final int port = 1;
    final String host = "::1";
    final DruidNode node = new DruidNode("service_name", host, port);
    final URL url = TaskStatusPostToLeaderListenerResource.makeReportUrl(node);
    Assert.assertEquals("[::1]", url.getHost());
    Assert.assertEquals(port, url.getPort());
    Assert.assertEquals("http", url.getProtocol());
    Assert.assertEquals(TaskStatusPostToLeaderListenerResource.PATH, url.getFile());
  }

  @Test
  public void testMakeReportUrlIPv6Bracketed() throws Exception
  {
    final int port = 1;
    final String host = "[::1]";
    final DruidNode node = new DruidNode("service_name", host, port);
    final URL url = TaskStatusPostToLeaderListenerResource.makeReportUrl(node);
    Assert.assertEquals(host, url.getHost());
    Assert.assertEquals(port, url.getPort());
    Assert.assertEquals("http", url.getProtocol());
    Assert.assertEquals(TaskStatusPostToLeaderListenerResource.PATH, url.getFile());
  }
}
