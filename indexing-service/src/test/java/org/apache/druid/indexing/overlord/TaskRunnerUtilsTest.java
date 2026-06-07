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
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class TaskRunnerUtilsTest
{
  @Test
  public void testMakeWorkerURL()
  {
    final URL url = TaskRunnerUtils.makeWorkerURL(
        new Worker("https", "1.2.3.4:8290", "1.2.3.4", 1, "0", WorkerConfig.DEFAULT_CATEGORY),
        "/druid/worker/v1/task/%s/log",
        "foo bar&"
    );
    Assert.assertEquals("https://1.2.3.4:8290/druid/worker/v1/task/foo%20bar%26/log", url.toString());
    Assert.assertEquals("1.2.3.4:8290", url.getAuthority());
    Assert.assertEquals("/druid/worker/v1/task/foo%20bar%26/log", url.getPath());
  }

  @Test
  public void testMakeTaskLocationURL()
  {
    final URL url = TaskRunnerUtils.makeTaskLocationURL(
        TaskLocation.create("1.2.3.4", 8090, 8290),
        "/druid/worker/v1/task/%s/log",
        "foo bar&"
    );
    Assert.assertEquals("https://1.2.3.4:8290/druid/worker/v1/task/foo%20bar%26/log", url.toString());
  }

  @Test
  public void testStreamTaskReportsFromTaskLocationOk() throws Exception
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final String report = "my report";
    EasyMock.expect(httpClient.go(EasyMock.anyObject(Request.class), EasyMock.anyObject(InputStreamFullResponseHandler.class)))
            .andReturn(Futures.immediateFuture(response(HttpResponseStatus.OK, report)));
    EasyMock.replay(httpClient);

    final Optional<InputStream> stream = TaskRunnerUtils.streamTaskReportsFromTaskLocation(
        httpClient,
        url("http://example.com/liveReports")
    );

    Assert.assertTrue(stream.isPresent());
    Assert.assertEquals(report, StringUtils.fromUtf8(ByteStreams.toByteArray(stream.get())));
    EasyMock.verify(httpClient);
  }

  @Test
  public void testStreamTaskReportsFromTaskLocationNotFound() throws Exception
  {
    assertStreamTaskReportsFromTaskLocationUnavailable(HttpResponseStatus.NOT_FOUND);
  }

  @Test
  public void testStreamTaskReportsFromTaskLocationServiceUnavailable() throws Exception
  {
    assertStreamTaskReportsFromTaskLocationUnavailable(HttpResponseStatus.SERVICE_UNAVAILABLE);
  }

  @Test
  public void testStreamTaskReportsFromTaskLocationUnexpectedStatus() throws Exception
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(Request.class), EasyMock.anyObject(InputStreamFullResponseHandler.class)))
            .andReturn(Futures.immediateFuture(response(HttpResponseStatus.INTERNAL_SERVER_ERROR, "error")));
    EasyMock.replay(httpClient);

    final IOException e = Assert.assertThrows(
        IOException.class,
        () -> TaskRunnerUtils.streamTaskReportsFromTaskLocation(
            httpClient,
            url("http://example.com/liveReports")
        )
    );

    Assert.assertTrue(e.getMessage().contains("500 Internal Server Error"));
    EasyMock.verify(httpClient);
  }

  private static void assertStreamTaskReportsFromTaskLocationUnavailable(
      final HttpResponseStatus status
  ) throws Exception
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(Request.class), EasyMock.anyObject(InputStreamFullResponseHandler.class)))
            .andReturn(Futures.immediateFuture(response(status, "error")));
    EasyMock.replay(httpClient);

    Assert.assertEquals(
        Optional.absent(),
        TaskRunnerUtils.streamTaskReportsFromTaskLocation(httpClient, url("http://example.com/liveReports"))
    );
    EasyMock.verify(httpClient);
  }

  private static URL url(final String value) throws IOException
  {
    return URI.create(value).toURL();
  }

  private static InputStreamFullResponseHolder response(
      final HttpResponseStatus status,
      final String content
  )
  {
    final InputStreamFullResponseHolder response = new InputStreamFullResponseHolder(
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    );
    response.addChunk(StringUtils.toUtf8(content));
    response.done();
    return response;
  }
}
