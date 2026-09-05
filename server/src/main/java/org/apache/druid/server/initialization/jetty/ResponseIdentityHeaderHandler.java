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
import org.eclipse.jetty.client.Response;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.rewrite.handler.HeaderPatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;

import javax.servlet.http.HttpServletResponse;

public class ResponseIdentityHeaderHandler extends RewriteHandler
{
  public static final String RESPONSE_SERVER_HEADER = "X-Druid-Response-Server";
  public static final String RESPONSE_SERVICE_HEADER = "X-Druid-Response-Service";

  public ResponseIdentityHeaderHandler(final DruidNode selfNode, final Handler handler)
  {
    super(handler);
    addRule(new HeaderPatternRule("*", RESPONSE_SERVER_HEADER, selfNode.getHostAndPortToUse()));
    addRule(new HeaderPatternRule("*", RESPONSE_SERVICE_HEADER, selfNode.getServiceName()));
  }

  public static void clearRouterIdentity(final HttpServletResponse proxyResponse)
  {
    // In EE8 compatible Jetty 12 using servlet API 4.x, setting a header to null is the accepted way to remove it.
    proxyResponse.setHeader(RESPONSE_SERVER_HEADER, null);
    proxyResponse.setHeader(RESPONSE_SERVICE_HEADER, null);
  }

  public static boolean shouldProxyIdentityHeader(
      final boolean responseIdentityHeadersEnabled,
      final Response serverResponse,
      final HttpField field
  )
  {
    if (!isIdentityHeader(field.getName())) {
      return true;
    }
    if (!responseIdentityHeadersEnabled) {
      return false;
    }

    final HttpFields headers = serverResponse.getHeaders();
    return headers.contains(RESPONSE_SERVER_HEADER) && headers.contains(RESPONSE_SERVICE_HEADER);
  }

  private static boolean isIdentityHeader(final String headerName)
  {
    return RESPONSE_SERVER_HEADER.equalsIgnoreCase(headerName)
           || RESPONSE_SERVICE_HEADER.equalsIgnoreCase(headerName);
  }
}
