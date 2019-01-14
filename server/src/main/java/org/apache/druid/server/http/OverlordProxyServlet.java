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

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.JettyUtils;
import org.apache.druid.server.security.AuthConfig;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.proxy.ProxyServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A Proxy servlet that proxies requests to the overlord.
 */
public class OverlordProxyServlet extends ProxyServlet
{
  private final DruidLeaderClient druidLeaderClient;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;

  @Inject
  OverlordProxyServlet(
      @IndexingService DruidLeaderClient druidLeaderClient,
      @Global Provider<HttpClient> httpClientProvider,
      @Global DruidHttpClientConfig httpClientConfig
  )
  {
    this.druidLeaderClient = druidLeaderClient;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    final String overlordLeader = druidLeaderClient.findCurrentLeader();
    if (overlordLeader == null) {
      throw new ISE("Can't find Overlord leader.");
    }

    return JettyUtils.concatenateForRewrite(
        overlordLeader,
        request.getRequestURI(),
        request.getQueryString()
    );
  }

  @Override
  protected HttpClient newHttpClient()
  {
    return httpClientProvider.get();
  }

  @Override
  protected HttpClient createHttpClient() throws ServletException
  {
    HttpClient client = super.createHttpClient();
    // override timeout set in ProxyServlet.createHttpClient
    setTimeout(httpClientConfig.getReadTimeout().getMillis());
    return client;
  }


  @Override
  protected void sendProxyRequest(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Request proxyRequest
  )
  {
    // Since we can't see the request object on the remote side, we can't check whether the remote side actually
    // performed an authorization check here, so always set this to true for the proxy servlet.
    // If the remote node failed to perform an authorization check, PreResponseAuthorizationCheckFilter
    // will log that on the remote node.
    clientRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    super.sendProxyRequest(
        clientRequest,
        proxyResponse,
        proxyRequest
    );
  }
}
