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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

public class ResponseIdentityHeaderHandlerTest
{
  @Test
  public void testRestoresIdentityHeadersAfterResponseResetAndHeaderClear() throws Exception
  {
    final DruidNode node = new DruidNode("druid/test", "test-host", false, 8080, null, true, false);
    final Request request = EasyMock.strictMock(Request.class);
    final org.eclipse.jetty.server.Response response = EasyMock.mock(org.eclipse.jetty.server.Response.class);
    final HttpFields.Mutable headers = HttpFields.build();

    EasyMock.expect(response.getHeaders()).andReturn(headers).times(2);
    response.reset();
    EasyMock.expectLastCall().andAnswer(
        () -> {
          headers.clear();
          return null;
        }
    );
    EasyMock.replay(request, response);

    final Handler handler = new Handler.Abstract.NonBlocking()
    {
      @Override
      public boolean handle(
          final Request request,
          final org.eclipse.jetty.server.Response response,
          final Callback callback
      )
      {
        response.getHeaders().clear();
        assertIdentityHeaders(response.getHeaders(), node);
        response.reset();
        return true;
      }
    };

    Assertions.assertTrue(new ResponseIdentityHeaderHandler(node, handler).handle(request, response, Callback.NOOP));
    assertIdentityHeaders(headers, node);
    EasyMock.verify(request, response);
  }

  @Test
  public void testClearRouterIdentityRemovesAllHeaders()
  {
    final HttpServletResponse proxyResponse = EasyMock.strictMock(HttpServletResponse.class);
    proxyResponse.setHeader(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, null);
    EasyMock.expectLastCall().once();
    proxyResponse.setHeader(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER, null);
    EasyMock.expectLastCall().once();
    proxyResponse.setHeader(ResponseIdentityHeaderHandler.RESPONSE_VERSION_HEADER, null);
    EasyMock.expectLastCall().once();

    EasyMock.replay(proxyResponse);
    ResponseIdentityHeaderHandler.clearRouterIdentity(proxyResponse);
    EasyMock.verify(proxyResponse);
  }

  @Test
  public void testShouldProxyIdentityHeaderWhenUpstreamReturnsAllHeaders()
  {
    final Response serverResponse = mockResponse(
        HttpFields.from(
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082"),
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER, "druid/broker"),
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_VERSION_HEADER, "39.0.0")
        ),
        1
    );

    Assertions.assertTrue(
        ResponseIdentityHeaderHandler.shouldProxyIdentityHeader(
            true,
            serverResponse,
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082")
        )
    );
    EasyMock.verify(serverResponse);
  }

  @Test
  public void testShouldNotProxyPartialIdentity()
  {
    final Response serverResponse = mockResponse(
        HttpFields.from(
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082"),
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER, "druid/broker")
        ),
        1
    );

    Assertions.assertFalse(
        ResponseIdentityHeaderHandler.shouldProxyIdentityHeader(
            true,
            serverResponse,
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082")
        )
    );
    EasyMock.verify(serverResponse);
  }

  @Test
  public void testShouldProxyUnrelatedHeader()
  {
    final Response serverResponse = EasyMock.strictMock(Response.class);
    EasyMock.replay(serverResponse);

    Assertions.assertTrue(
        ResponseIdentityHeaderHandler.shouldProxyIdentityHeader(
            false,
            serverResponse,
            new HttpField("Content-Type", "application/json")
        )
    );
    EasyMock.verify(serverResponse);
  }

  @Test
  public void testShouldNotProxyIdentityWhenRouterFeatureIsDisabled()
  {
    final Response serverResponse = EasyMock.strictMock(Response.class);
    EasyMock.replay(serverResponse);

    Assertions.assertFalse(
        ResponseIdentityHeaderHandler.shouldProxyIdentityHeader(
            false,
            serverResponse,
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082")
        )
    );
    EasyMock.verify(serverResponse);
  }

  private static Response mockResponse(final HttpFields headers, final int calls)
  {
    final Response serverResponse = EasyMock.strictMock(Response.class);
    EasyMock.expect(serverResponse.getHeaders()).andReturn(headers).times(calls);
    EasyMock.replay(serverResponse);
    return serverResponse;
  }

  private static void assertIdentityHeaders(final HttpFields headers, final DruidNode node)
  {
    Assertions.assertEquals("test-host:8080", headers.get(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER));
    Assertions.assertEquals("druid/test", headers.get(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER));
    Assertions.assertEquals(node.getVersion(), headers.get(ResponseIdentityHeaderHandler.RESPONSE_VERSION_HEADER));
  }
}
