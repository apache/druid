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

package org.apache.druid.sql.avatica;

import org.apache.druid.server.DruidNode;
import org.easymock.EasyMock;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.junit.Assert;
import org.junit.Test;

public class DruidAvaticaJsonHandlerTest extends DruidAvaticaHandlerTest
{
  @Override
  protected String getJdbcUrlTail()
  {
    return DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  @Override
  protected Handler.Abstract getAvaticaHandler(final DruidMeta druidMeta)
  {
    return new DruidAvaticaJsonHandler(
            druidMeta,
            new DruidNode("dummy", "dummy", false, 1, null, true, false),
            new AvaticaMonitor()
    );
  }

  @Test
  public void testNonPostRequestSucceeds() throws Exception
  {
    DruidMeta druidMeta = EasyMock.mock(DruidMeta.class);
    DruidAvaticaJsonHandler handler = new DruidAvaticaJsonHandler(
        druidMeta,
        new DruidNode("dummy", "dummy", false, 1, null, true, false),
        new AvaticaMonitor()
    );

    Request request = EasyMock.mock(Request.class);
    Response response = EasyMock.mock(Response.class);
    Callback callback = EasyMock.mock(Callback.class);
    HttpURI httpURI = EasyMock.mock(HttpURI.class);
    HttpFields.Mutable headers = EasyMock.mock(HttpFields.Mutable.class);

    EasyMock.expect(request.getHttpURI()).andReturn(httpURI);
    EasyMock.expect(httpURI.getPath()).andReturn(DruidAvaticaJsonHandler.AVATICA_PATH_NO_TRAILING_SLASH);
    EasyMock.expect(request.getMethod()).andReturn("GET");

    EasyMock.expect(response.getHeaders()).andReturn(headers);
    
    headers.put("Content-Type", "application/json;charset=utf-8");
    EasyMock.expectLastCall().andReturn(null);
    
    callback.succeeded();
    EasyMock.expectLastCall();

    EasyMock.replay(request, response, callback, httpURI);

    boolean handled = handler.handle(request, response, callback);

    Assert.assertTrue("Handler should have handled the request", handled);
    EasyMock.verify(request, response, callback, httpURI);
  }
}
