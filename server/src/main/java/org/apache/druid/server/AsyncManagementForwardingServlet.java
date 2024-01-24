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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.initialization.jetty.StandardResponseHeaderFilterHolder;
import org.apache.druid.server.security.AuthConfig;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.proxy.AsyncProxyServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AsyncManagementForwardingServlet extends AsyncProxyServlet
{
  private static final String BASE_URI_ATTRIBUTE = "org.apache.druid.proxy.to.base.uri";
  private static final String MODIFIED_PATH_ATTRIBUTE = "org.apache.druid.proxy.to.path";

  // These are the typical path conventions for the coordinator and overlord APIs. If we see one of these paths, we will
  // forward the request with the path unmodified, e.g.:
  //   Client Request: https://{ROUTER_HOST}:9088/druid/coordinator/v1/loadstatus?full
  //   Proxy Request:  https://{COORDINATOR_HOST}:8281/druid/coordinator/v1/loadstatus?full
  private static final String STANDARD_COORDINATOR_BASE_PATH = "/druid/coordinator";
  private static final String STANDARD_OVERLORD_BASE_PATH = "/druid/indexer";

  // But there are some cases where the path is either ambiguous or collides with other servlet pathSpecs and where it
  // is desirable to explicitly state the destination host. In these cases, we will forward the request with the proxy
  // destination component of the path stripped, e.g.:
  //   Client Request: https://{ROUTER_HOST}:9088/proxy/coordinator/druid-ext/basic-security/authorization/db/b/users
  //   Proxy Request:  https://{COORDINATOR_HOST}:8281/druid-ext/basic-security/authorization/db/b/users
  private static final String ARBITRARY_COORDINATOR_BASE_PATH = "/proxy/coordinator";
  private static final String ARBITRARY_OVERLORD_BASE_PATH = "/proxy/overlord";

  // This path is used to check if the managment proxy is enabled, it simply returns {"enabled":true}
  private static final String ENABLED_PATH = "/proxy/enabled";

  private final ObjectMapper jsonMapper;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final DruidLeaderSelector coordLeaderSelector;
  private final DruidLeaderSelector overlordLeaderSelector;

  @Inject
  public AsyncManagementForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Global Provider<HttpClient> httpClientProvider,
      @Global DruidHttpClientConfig httpClientConfig,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      @IndexingService DruidLeaderSelector overlordLeaderSelector
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.coordLeaderSelector = coordLeaderSelector;
    this.overlordLeaderSelector = overlordLeaderSelector;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    String currentLeader;
    String requestURI = StringUtils.toLowerCase(request.getRequestURI());
    if (requestURI.startsWith(STANDARD_COORDINATOR_BASE_PATH)) {
      currentLeader = coordLeaderSelector.getCurrentLeader();
    } else if (requestURI.startsWith(STANDARD_OVERLORD_BASE_PATH)) {
      currentLeader = overlordLeaderSelector.getCurrentLeader();
    } else if (requestURI.startsWith(ARBITRARY_COORDINATOR_BASE_PATH)) {
      currentLeader = coordLeaderSelector.getCurrentLeader();
      request.setAttribute(
          MODIFIED_PATH_ATTRIBUTE,
          request.getRequestURI().substring(ARBITRARY_COORDINATOR_BASE_PATH.length())
      );
    } else if (requestURI.startsWith(ARBITRARY_OVERLORD_BASE_PATH)) {
      currentLeader = overlordLeaderSelector.getCurrentLeader();
      request.setAttribute(
          MODIFIED_PATH_ATTRIBUTE,
          request.getRequestURI().substring(ARBITRARY_OVERLORD_BASE_PATH.length())
      );
    } else if (ENABLED_PATH.equals(requestURI)) {
      handleEnabledRequest(response);
      return;
    } else {
      handleInvalidRequest(
          response,
          StringUtils.format("Unsupported proxy destination[%s]", request.getRequestURI()),
          HttpServletResponse.SC_BAD_REQUEST
      );
      return;
    }

    if (currentLeader == null) {
      handleInvalidRequest(
          response,
          StringUtils.format(
              "Unable to determine destination[%s]; is your coordinator/overlord running?",
              request.getRequestURI()
          ),
          HttpServletResponse.SC_SERVICE_UNAVAILABLE
      );
      return;
    }

    request.setAttribute(BASE_URI_ATTRIBUTE, currentLeader);
    super.service(request, response);
  }

  @Override
  protected void sendProxyRequest(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Request proxyRequest
  )
  {
    proxyRequest.timeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);
    proxyRequest.idleTimeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);

    clientRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true); // auth is handled on the destination host

    super.sendProxyRequest(clientRequest, proxyResponse, proxyRequest);
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    final String encodedPath = request.getAttribute(MODIFIED_PATH_ATTRIBUTE) != null
                               ? (String) request.getAttribute(MODIFIED_PATH_ATTRIBUTE)
                               : request.getRequestURI();

    return JettyUtils.concatenateForRewrite(
        (String) request.getAttribute(BASE_URI_ATTRIBUTE),
        encodedPath,
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
    setTimeout(httpClientConfig.getReadTimeout().getMillis()); // override timeout set in ProxyServlet.createHttpClient
    return client;
  }

  @Override
  protected void onServerResponseHeaders(
      HttpServletRequest clientRequest,
      HttpServletResponse proxyResponse,
      Response serverResponse
  )
  {
    StandardResponseHeaderFilterHolder.deduplicateHeadersInProxyServlet(proxyResponse, serverResponse);
    super.onServerResponseHeaders(clientRequest, proxyResponse, serverResponse);
  }

  private void handleInvalidRequest(HttpServletResponse response, String errorMessage, int statusCode) throws IOException
  {
    if (!response.isCommitted()) {
      response.resetBuffer();
      response.setStatus(statusCode);
      jsonMapper.writeValue(response.getOutputStream(), ImmutableMap.of("error", errorMessage));
    }
    response.flushBuffer();
  }

  private void handleEnabledRequest(HttpServletResponse response) throws IOException
  {
    if (!response.isCommitted()) {
      response.resetBuffer();
      response.setStatus(HttpServletResponse.SC_OK);
      jsonMapper.writeValue(response.getOutputStream(), ImmutableMap.of("enabled", true));
    }
    response.flushBuffer();
  }
}
