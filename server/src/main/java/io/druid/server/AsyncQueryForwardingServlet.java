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
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.metrics.QueryCountStatsProvider;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.joda.time.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
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
  private static final String QUERY_ATTRIBUTE = "io.druid.proxy.query";
  private static final String OBJECTMAPPER_ATTRIBUTE = "io.druid.proxy.objectMapper";

  private static final int CANCELLATION_TIMEOUT_MILLIS = 500;
  private static final int MAX_QUEUED_CANCELLATIONS = 64;
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

  private HttpClient broadcastClient;

  @Inject
  public AsyncQueryForwardingServlet(
      QueryToolChestWarehouse warehouse,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      GenericQueryMetricsFactory queryMetricsFactory
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
  }

  @Override
  public void init() throws ServletException
  {
    super.init();

    // separate client with more aggressive connection timeouts
    // to prevent cancellations requests from blocking queries
    broadcastClient = httpClientProvider.get();
    broadcastClient.setConnectTimeout(CANCELLATION_TIMEOUT_MILLIS);
    broadcastClient.setMaxRequestsQueuedPerDestination(MAX_QUEUED_CANCELLATIONS);

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

    final String defaultHost = hostFinder.getDefaultHost();
    request.setAttribute(HOST_ATTRIBUTE, defaultHost);

    final boolean isQueryEndpoint = request.getRequestURI().startsWith("/druid/v2");

    if (isQueryEndpoint && HttpMethod.DELETE.is(request.getMethod())) {
      // query cancellation request
      for (final String host : hostFinder.getAllHosts()) {
        // send query cancellation to all brokers this query may have gone to
        // to keep the code simple, the proxy servlet will also send a request to one of the default brokers
        if (!host.equals(defaultHost)) {
          // issue async requests
          broadcastClient
              .newRequest(rewriteURI(request, host))
              .method(HttpMethod.DELETE)
              .timeout(CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
              .send(
                  new Response.CompleteListener()
                  {
                    @Override
                    public void onComplete(Result result)
                    {
                      if (result.isFailed()) {
                        log.warn(
                            result.getFailure(),
                            "Failed to forward cancellation request to [%s]",
                            host
                        );
                      }
                    }
                  }
              );
        }
        interruptedQueryCount.incrementAndGet();
      }
    } else if (isQueryEndpoint && HttpMethod.POST.is(request.getMethod())) {
      // query request
      try {
        Query inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        if (inputQuery != null) {
          request.setAttribute(HOST_ATTRIBUTE, hostFinder.getHost(inputQuery));
          if (inputQuery.getId() == null) {
            inputQuery = inputQuery.withId(UUID.randomUUID().toString());
          }
        }
        request.setAttribute(QUERY_ATTRIBUTE, inputQuery);
      }
      catch (IOException e) {
        log.warn(e, "Exception parsing query");
        final String errorMessage = e.getMessage() == null ? "no error message" : e.getMessage();
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
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
    }

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
    return rewriteURI(request, (String) request.getAttribute(HOST_ATTRIBUTE)).toString();
  }

  protected URI rewriteURI(HttpServletRequest request, String host)
  {
    return makeURI(host, request.getRequestURI(), request.getQueryString());
  }

  protected static URI makeURI(String host, String requestURI, String rawQueryString)
  {
    try {
      return new URI(
          "http",
          host,
          requestURI,
          rawQueryString == null ? null : URLDecoder.decode(rawQueryString, "UTF-8"),
          null
      );
    }
    catch (UnsupportedEncodingException | URISyntaxException e) {
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
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "query/time",
                        TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                        "success",
                        success
                        && result.getResponse().getStatus() == javax.ws.rs.core.Response.Status.OK.getStatusCode()
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
                new DateTime(),
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

    private void emitQueryTime(long requestTimeNs, boolean success) throws JsonProcessingException
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
