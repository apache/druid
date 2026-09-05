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

import org.easymock.EasyMock;
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

public class ResponseIdentityHeaderHandlerTest
{
  @Test
  public void testClearRouterIdentityRemovesBothHeaders()
  {
    final HttpServletResponse proxyResponse = EasyMock.strictMock(HttpServletResponse.class);
    proxyResponse.setHeader(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, null);
    EasyMock.expectLastCall().once();
    proxyResponse.setHeader(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER, null);
    EasyMock.expectLastCall().once();

    EasyMock.replay(proxyResponse);
    ResponseIdentityHeaderHandler.clearRouterIdentity(proxyResponse);
    EasyMock.verify(proxyResponse);
  }

  @Test
  public void testShouldProxyIdentityHeaderWhenUpstreamReturnsBothHeaders()
  {
    final Response serverResponse = mockResponse(
        HttpFields.from(
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082"),
            new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER, "druid/broker")
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
        HttpFields.from(new HttpField(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER, "upstream:8082")),
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
}
