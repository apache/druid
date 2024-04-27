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

package org.apache.druid.server.http;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

public class OverlordProxyServletTest
{
  @Test
  public void testRewriteURI() throws URISyntaxException
  {
    OverlordClient overlordClient = EasyMock.createMock(OverlordClient.class);
    EasyMock.expect(overlordClient.findCurrentLeader())
            .andReturn(Futures.immediateFuture(new URI("https://overlord:port")));

    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getQueryString()).andReturn("param1=test&param2=test2").anyTimes();

    // %3A is a colon; test to make sure urlencoded paths work right.
    EasyMock.expect(request.getRequestURI()).andReturn("/druid/over%3Alord/worker").anyTimes();

    EasyMock.replay(overlordClient, request);

    URI uri = URI.create(new OverlordProxyServlet(overlordClient, null, null).rewriteTarget(request));
    Assert.assertEquals("https://overlord:port/druid/over%3Alord/worker?param1=test&param2=test2", uri.toString());

    EasyMock.verify(overlordClient, request);
  }
}
