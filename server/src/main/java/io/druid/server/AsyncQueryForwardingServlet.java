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

package io.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.client.selector.Server;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.metrics.QueryCountStatsProvider;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.proxy.AsyncProxyServlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet implements QueryCountStatsProvider
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  private static final String APPLICATION_SMILE = "application/smile";

  private static final String HOST_ATTRIBUTE = "io.druid.proxy.to.host";
  private static final String SCHEME_ATTRIBUTE = "io.druid.proxy.to.host.scheme";
  private static final String QUERY_ATTRIBUTE = "io.druid.proxy.query";
  private static final String AVATICA_QUERY_ATTRIBUTE = "io.druid.proxy.avaticaQuery";
  private static final String OBJECTMAPPER_ATTRIBUTE = "io.druid.proxy.objectMapper";

  private static final int CANCELLATION_TIMEOUT_MILLIS = 500;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();

  private static void handleException(HttpServletResponse response, ObjectMapper objectMapper, Exception exception)
      throws IOException
  {
    if (!response.isCommitted()) {
      final String errorMessage = exception.getMessage() == null ? "null exception" : exception.getMessage();

      response.resetBuffer();
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      objectMapper.writeValue(
          response.getOutputStream(),
          ImmutableMap.of("error", errorMessage)
      );
    }
    response.flushBuffer();
  }

  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final AuthenticatorMapper authenticatorMapper;

  private HttpClient broadcastClient;

  @Inject
  public AsyncQueryForwardingServlet(
      QueryToolChestWarehouse warehouse,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      @Router DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      GenericQueryMetricsFactory queryMetricsFactory,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.warehouse = warehouse;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.authenticatorMapper = authenticatorMapper;
  }

  @Override
  public void init() throws ServletException
  {
    super.init();

    // Note that httpClientProvider is setup to return same HttpClient instance on each get() so
    // it is same http client as that is used by parent ProxyServlet.
    broadcastClient = newHttpClient();
    try {
      broadcastClient.start();
    }
    catch (Exception e) {
      throw new ServletException(e);
    }
  }

  @Override
  public void destroy()
  {
    super.destroy();
    try {
      broadcastClient.stop();
    }
    catch (Exception e) {
      log.warn(e, "Error stopping servlet");
    }
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType())
                            || APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    request.setAttribute(OBJECTMAPPER_ATTRIBUTE, objectMapper);

    final String requestURI = request.getRequestURI();
    final String method = request.getMethod();
    final Server targetServer;

    // The Router does not have the ability to look inside SQL queries and route them intelligently, so just treat
    // them as a generic request.
    final boolean isQueryEndpoint = requestURI.startsWith("/druid/v2") && !requestURI.startsWith("/druid/v2/sql");

    final boolean isAvatica = requestURI.startsWith("/druid/v2/sql/avatica");

    if (isAvatica) {
      Map<String, Object> requestMap = objectMapper.readValue(
          request.getInputStream(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      String connectionId = getAvaticaConnectionId(requestMap);
      targetServer = hostFinder.findServerAvatica(connectionId);
      byte[] requestBytes = objectMapper.writeValueAsBytes(requestMap);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
    } else if (isQueryEndpoint && HttpMethod.DELETE.is(method)) {
      // query cancellation request
      targetServer = hostFinder.pickDefaultServer();

      for (final Server server : hostFinder.getAllServers()) {
        // send query cancellation to all brokers this query may have gone to
        // to keep the code simple, the proxy servlet will also send a request to the default targetServer.
        if (!server.getHost().equals(targetServer.getHost())) {
          // issue async requests
          Response.CompleteListener completeListener = result -> {
            if (result.isFailed()) {
              log.warn(
                  result.getFailure(),
                  "Failed to forward cancellation request to [%s]",
                  server.getHost()
              );
            }
          };

          Request broadcastReq = broadcastClient
              .newRequest(rewriteURI(request, server.getScheme(), server.getHost()))
              .method(HttpMethod.DELETE)
              .timeout(CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

          copyRequestHeaders(request, broadcastReq);
          broadcastReq.send(completeListener);
        }
        interruptedQueryCount.incrementAndGet();
      }
    } else if (isQueryEndpoint && HttpMethod.POST.is(method)) {
      // query request
      try {
        Query inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        if (inputQuery != null) {
          targetServer = hostFinder.pickServer(inputQuery);
          if (inputQuery.getId() == null) {
            inputQuery = inputQuery.withId(UUID.randomUUID().toString());
          }
        } else {
          targetServer = hostFinder.pickDefaultServer();
        }
        request.setAttribute(QUERY_ATTRIBUTE, inputQuery);
      }
      catch (IOException e) {
        log.warn(e, "Exception parsing query");
        final String errorMessage = e.getMessage() == null ? "no error message" : e.getMessage();
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                request.getRemoteAddr(),
                null,
                new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", errorMessage))
            )
        );
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentType(MediaType.APPLICATION_JSON);
        objectMapper.writeValue(
            response.getOutputStream(),
            ImmutableMap.of("error", errorMessage)
        );

        return;
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    } else {
      targetServer = hostFinder.pickDefaultServer();
    }

    request.setAttribute(HOST_ATTRIBUTE, targetServer.getHost());
    request.setAttribute(SCHEME_ATTRIBUTE, targetServer.getScheme());

    doService(request, response);
  }

  protected void doService(
      HttpServletRequest request,
      HttpServletResponse response
  ) throws ServletException, IOException
  {
    // Just call the superclass service method. Overriden in tests.
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

    byte[] avaticaQuery = (byte[]) clientRequest.getAttribute(AVATICA_QUERY_ATTRIBUTE);
    if (avaticaQuery != null) {
      proxyRequest.content(new BytesContentProvider(avaticaQuery));
    }

    final Query query = (Query) clientRequest.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      final ObjectMapper objectMapper = (ObjectMapper) clientRequest.getAttribute(OBJECTMAPPER_ATTRIBUTE);
      try {
        proxyRequest.content(new BytesContentProvider(objectMapper.writeValueAsBytes(query)));
      }
      catch (JsonProcessingException e) {
        Throwables.propagate(e);
      }
    }

    // Since we can't see the request object on the remote side, we can't check whether the remote side actually
    // performed an authorization check here, so always set this to true for the proxy servlet.
    // If the remote node failed to perform an authorization check, PreResponseAuthorizationCheckFilter
    // will log that on the remote node.
    clientRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

    // Check if there is an authentication result and use it to decorate the proxy request if needed.
    AuthenticationResult authenticationResult = (AuthenticationResult) clientRequest.getAttribute(
        AuthConfig.DRUID_AUTHENTICATION_RESULT);
    if (authenticationResult != null && authenticationResult.getAuthenticatedBy() != null) {
      Authenticator authenticator = authenticatorMapper.getAuthenticatorMap()
                                                       .get(authenticationResult.getAuthenticatedBy());
      if (authenticator != null) {
        authenticator.decorateProxyRequest(
            clientRequest,
            proxyResponse,
            proxyRequest
        );
      } else {
        log.error("Can not find Authenticator with Name [%s]", authenticationResult.getAuthenticatedBy());
      }
    }
    super.sendProxyRequest(
        clientRequest,
        proxyResponse,
        proxyRequest
    );
  }

  @Override
  protected Response.Listener newProxyResponseListener(
      HttpServletRequest request, HttpServletResponse response
  )
  {
    final Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      return newMetricsEmittingProxyResponseListener(request, response, query, System.nanoTime());
    } else {
      return super.newProxyResponseListener(request, response);
    }
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request)
  {
    return rewriteURI(
        request,
        (String) request.getAttribute(SCHEME_ATTRIBUTE),
        (String) request.getAttribute(HOST_ATTRIBUTE)
    ).toString();
  }

  protected URI rewriteURI(HttpServletRequest request, String scheme, String host)
  {
    return makeURI(scheme, host, request.getRequestURI(), request.getQueryString());
  }

  protected static URI makeURI(String scheme, String host, String requestURI, String rawQueryString)
  {
    try {
      return new URIBuilder()
          .setScheme(scheme)
          .setHost(host)
          .setPath(requestURI)
          // No need to encode-decode queryString, it is already encoded
          .setQuery(rawQueryString)
          .build();
    }
    catch (URISyntaxException e) {
      log.error(e, "Unable to rewrite URI [%s]", e.getMessage());
      throw Throwables.propagate(e);
    }
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

  private Response.Listener newMetricsEmittingProxyResponseListener(
      HttpServletRequest request,
      HttpServletResponse response,
      Query query,
      long startNs
  )
  {
    return new MetricsEmittingProxyResponseListener(request, response, query, startNs);
  }

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  private static String getAvaticaConnectionId(Map<String, Object> requestMap)
  {
    Object connectionIdObj = requestMap.get("connectionId");
    if (connectionIdObj == null) {
      throw new IAE("Received an Avatica request without a connectionId.");
    }
    if (!(connectionIdObj instanceof String)) {
      throw new IAE("Received an Avatica request with a non-String connectionId.");
    }

    return (String) connectionIdObj;
  }

  private class MetricsEmittingProxyResponseListener extends ProxyResponseListener
  {
    private final HttpServletRequest req;
    private final HttpServletResponse res;
    private final Query query;
    private final long startNs;

    public MetricsEmittingProxyResponseListener(
        HttpServletRequest request,
        HttpServletResponse response,
        Query query,
        long startNs
    )
    {
      super(request, response);

      this.req = request;
      this.res = response;
      this.query = query;
      this.startNs = startNs;
    }

    @Override
    public void onComplete(Result result)
    {
      final long requestTimeNs = System.nanoTime() - startNs;
      try {
        boolean success = result.isSucceeded();
        if (success) {
          successfulQueryCount.incrementAndGet();
        } else {
          failedQueryCount.incrementAndGet();
        }
        emitQueryTime(requestTimeNs, success);
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "query/time",
                        TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                        "success",
                        success
                        && result.getResponse().getStatus() == Status.OK.getStatusCode()
                    )
                )
            )
        );
      }
      catch (Exception e) {
        log.error(e, "Unable to log query [%s]!", query);
      }

      super.onComplete(result);
    }

    @Override
    public void onFailure(Response response, Throwable failure)
    {
      try {
        final String errorMessage = failure.getMessage();
        failedQueryCount.incrementAndGet();
        emitQueryTime(System.nanoTime() - startNs, false);
        requestLogger.log(
            new RequestLogLine(
                DateTimes.nowUtc(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "success",
                        false,
                        "exception",
                        errorMessage == null ? "no message" : errorMessage
                    )
                )
            )
        );
      }
      catch (IOException logError) {
        log.error(logError, "Unable to log query [%s]!", query);
      }

      log.makeAlert(failure, "Exception handling request")
         .addData("exception", failure.toString())
         .addData("query", query)
         .addData("peer", req.getRemoteAddr())
         .emit();

      super.onFailure(response, failure);
    }

    private void emitQueryTime(long requestTimeNs, boolean success)
    {
      QueryMetrics queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          warehouse.getToolChest(query),
          query,
          req.getRemoteAddr()
      );
      queryMetrics.success(success);
      queryMetrics.reportQueryTime(requestTimeNs).emit(emitter);
    }
  }
}
