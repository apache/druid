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

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.apache.druid.java.util.common.logger.Logger

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JettyRequestLogTest
{
  @Test
  public void testLog()
  {
    Request request = Mockito.mock(Request.class);
    Mockito.when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    Mockito.when(request.getMethod()).thenReturn("GET");
    Mockito.when(request.getHttpURI()).thenReturn("http://whatever");
    Mockito.when(request.getProtocol()).thenReturn("http");
    
    Response response = Mockito.mock(Response.class);
    Mockito.when(response.getStatus()).thenReturn(200);

    JettyRequestLog jettyRequestLog = new JettyRequestLog(request, response);
    private static final Logger logger = new Logger("org.apache.druid.jetty.RequestLog");

    jettyRequestLog.log()
    verify(Logger, times(1)).debug(
        "%s %s %s %s %d",
        request.getRemoteAddr(),
        request.getMethod(),
        request.getHttpURI().toString(),
        request.getProtocol(),
        response.getStatus()
    )
  }
}
