/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.http;

import io.druid.discovery.DruidLeaderClient;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;

public class OverlordProxyServletTest
{
  @Test
  public void testRewriteURI()
  {
    DruidLeaderClient druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);
    EasyMock.expect(druidLeaderClient.findCurrentLeader()).andReturn("https://overlord:port");

    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getQueryString()).andReturn("param1=test&param2=test2").anyTimes();
    EasyMock.expect(request.getRequestURI()).andReturn("/druid/overlord/worker").anyTimes();

    EasyMock.replay(druidLeaderClient, request);

    URI uri = URI.create(new OverlordProxyServlet(druidLeaderClient).rewriteTarget(request));
    Assert.assertEquals("https://overlord:port/druid/overlord/worker?param1=test&param2=test2", uri.toString());
  }

}
