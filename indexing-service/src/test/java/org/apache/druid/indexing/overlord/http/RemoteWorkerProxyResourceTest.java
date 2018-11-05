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

package org.apache.druid.indexing.overlord.http;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.net.URL;


public class RemoteWorkerProxyResourceTest
{
  private RemoteWorkerProxyResource remoteWorkerProxyResource;
  private HttpClient httpClient;

  @Before
  public void setUp()
  {
    httpClient = EasyMock.createMock(HttpClient.class);
    remoteWorkerProxyResource = new RemoteWorkerProxyResource(httpClient);
  }

  @Test
  public void testDoDisable() throws Exception
  {
    final URL url = new URL("http://foo:8001/druid/worker/v1/disable");
    String workerResponse = "{\"foo:8001\":\"disabled\"}";

    Capture<Request> capturedRequest = getRequestCapture(HttpResponseStatus.OK, workerResponse);

    Response response = remoteWorkerProxyResource.doDisable("http", "foo:8001");

    Assert.assertEquals(workerResponse, response.getEntity());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());

    EasyMock.verify(httpClient);
  }

  @Test
  public void testDoDisableWhenWorkerRaisesError() throws Exception
  {
    final URL url = new URL("http://foo:8001/druid/worker/v1/disable");

    Capture<Request> capturedRequest = getRequestCapture(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");

    Response response = remoteWorkerProxyResource.doDisable("http", "foo:8001");

    Assert.assertEquals("", response.getEntity());
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());

    EasyMock.verify(httpClient);
  }

  @Test
  public void testDoEnable() throws Exception
  {
    final URL url = new URL("http://foo:8001/druid/worker/v1/enable");
    String workerResponse = "{\"foo:8001\":\"enabled\"}";

    Capture<Request> capturedRequest = getRequestCapture(HttpResponseStatus.OK, workerResponse);

    Response response = remoteWorkerProxyResource.doEnable("http", "foo:8001");

    Assert.assertEquals(workerResponse, response.getEntity());
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());

    EasyMock.verify(httpClient);
  }

  @Test
  public void testDoEnableWhenWorkerRaisesError() throws Exception
  {
    final URL url = new URL("http://foo:8001/druid/worker/v1/enable");

    Capture<Request> capturedRequest = getRequestCapture(HttpResponseStatus.INTERNAL_SERVER_ERROR, "");

    Response response = remoteWorkerProxyResource.doEnable("http", "foo:8001");

    Assert.assertEquals("", response.getEntity());
    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assert.assertEquals(HttpMethod.POST, capturedRequest.getValue().getMethod());
    Assert.assertEquals(url, capturedRequest.getValue().getUrl());

    EasyMock.verify(httpClient);
  }

  private Capture<Request> getRequestCapture(HttpResponseStatus httpStatus, String responseContent)
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
            .times(1);

    EasyMock.replay(httpClient);
    return capturedRequest;
  }
}
