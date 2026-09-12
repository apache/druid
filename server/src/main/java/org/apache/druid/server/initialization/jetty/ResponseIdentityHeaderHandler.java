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
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ResponseIdentityHeaderHandler extends Handler.Wrapper
{
  private static final String LOCAL_IDENTITY_ATTRIBUTE =
      ResponseIdentityHeaderHandler.class.getName() + ".localIdentity";

  public static final String RESPONSE_SERVER_HEADER = "X-Druid-Server";
  public static final String RESPONSE_SERVICE_HEADER = "X-Druid-Service";
  public static final String RESPONSE_VERSION_HEADER = "X-Druid-Version";

  private final String responseServer;
  private final String responseService;
  private final String responseVersion;

  public ResponseIdentityHeaderHandler(final DruidNode selfNode, final Handler handler)
  {
    super(handler);
    responseServer = selfNode.getHostAndPortToUse();
    responseService = selfNode.getServiceName();
    responseVersion = selfNode.getVersion();
  }

  @Override
  public boolean handle(
      final Request request,
      final org.eclipse.jetty.server.Response response,
      final Callback callback
  ) throws Exception
  {
    addIdentityHeaders(response);
    final HttpFields.Mutable responseHeaders = new HttpFields.Mutable.Wrapper(response.getHeaders())
    {
      @Override
      public HttpFields.Mutable clear()
      {
        super.clear();
        addIdentityHeaders(this);
        return this;
      }
    };
    return super.handle(
        request,
        new org.eclipse.jetty.server.Response.Wrapper(request, response)
        {
          @Override
          public HttpFields.Mutable getHeaders()
          {
            return responseHeaders;
          }

          @Override
          public void reset()
          {
            super.reset();
            addIdentityHeaders(this);
          }
        },
        callback
    );
  }

  private void addIdentityHeaders(final org.eclipse.jetty.server.Response response)
  {
    addIdentityHeaders(response.getHeaders());
  }

  private void addIdentityHeaders(final HttpFields.Mutable headers)
  {
    headers.put(RESPONSE_SERVER_HEADER, responseServer);
    headers.put(RESPONSE_SERVICE_HEADER, responseService);
    headers.put(RESPONSE_VERSION_HEADER, responseVersion);
  }

  public static void addIdentityHeaders(
      final org.eclipse.jetty.server.Response response,
      final DruidNode selfNode
  )
  {
    response.getHeaders().put(RESPONSE_SERVER_HEADER, selfNode.getHostAndPortToUse());
    response.getHeaders().put(RESPONSE_SERVICE_HEADER, selfNode.getServiceName());
    response.getHeaders().put(RESPONSE_VERSION_HEADER, selfNode.getVersion());
  }

  public static void rememberLocalIdentity(
      final HttpServletRequest clientRequest,
      final HttpServletResponse proxyResponse
  )
  {
    final String server = proxyResponse.getHeader(RESPONSE_SERVER_HEADER);
    final String service = proxyResponse.getHeader(RESPONSE_SERVICE_HEADER);
    final String version = proxyResponse.getHeader(RESPONSE_VERSION_HEADER);
    if (server != null && service != null && version != null) {
      clientRequest.setAttribute(LOCAL_IDENTITY_ATTRIBUTE, new ResponseIdentity(server, service, version));
    }
  }

  public static void restoreLocalIdentity(
      final HttpServletRequest clientRequest,
      final HttpServletResponse proxyResponse
  )
  {
    final Object identity = clientRequest.getAttribute(LOCAL_IDENTITY_ATTRIBUTE);
    if (identity instanceof ResponseIdentity localIdentity) {
      clearRouterIdentity(proxyResponse);
      proxyResponse.setHeader(RESPONSE_SERVER_HEADER, localIdentity.server());
      proxyResponse.setHeader(RESPONSE_SERVICE_HEADER, localIdentity.service());
      proxyResponse.setHeader(RESPONSE_VERSION_HEADER, localIdentity.version());
    }
  }

  private record ResponseIdentity(String server, String service, String version)
  {
  }

  public static void clearRouterIdentity(final HttpServletResponse proxyResponse)
  {
    // In EE8 compatible Jetty 12 using servlet API 4.x, setting a header to null is the accepted way to remove it.
    proxyResponse.setHeader(RESPONSE_SERVER_HEADER, null);
    proxyResponse.setHeader(RESPONSE_SERVICE_HEADER, null);
    proxyResponse.setHeader(RESPONSE_VERSION_HEADER, null);
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
    return headers.contains(RESPONSE_SERVER_HEADER)
           && headers.contains(RESPONSE_SERVICE_HEADER)
           && headers.contains(RESPONSE_VERSION_HEADER);
  }

  private static boolean isIdentityHeader(final String headerName)
  {
    return RESPONSE_SERVER_HEADER.equalsIgnoreCase(headerName)
           || RESPONSE_SERVICE_HEADER.equalsIgnoreCase(headerName)
           || RESPONSE_VERSION_HEADER.equalsIgnoreCase(headerName);
  }
}
