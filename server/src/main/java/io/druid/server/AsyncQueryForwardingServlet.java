/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.query.Query;
import io.druid.query.QueryMetricUtil;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
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
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  private static final String APPLICATION_SMILE = "application/smile";

  private static final String HOST_ATTRIBUTE = "io.druid.proxy.to.host";
  private static final String QUERY_ATTRIBUTE = "io.druid.proxy.query";
  private static final String OBJECTMAPPER_ATTRIBUTE = "io.druid.proxy.objectMapper";

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

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  public AsyncQueryForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType()) || APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    request.setAttribute(OBJECTMAPPER_ATTRIBUTE, objectMapper);

    String host = hostFinder.getDefaultHost();
    request.setAttribute(HOST_ATTRIBUTE, host);

    boolean isQuery = request.getMethod().equals(HttpMethod.POST.asString()) &&
                      request.getRequestURI().startsWith("/druid/v2");

    // queries only exist for POST
    if (isQuery) {
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
  protected void customizeProxyRequest(Request proxyRequest, HttpServletRequest request)
  {
    proxyRequest.timeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);
    proxyRequest.idleTimeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);

    final Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      final ObjectMapper objectMapper = (ObjectMapper) request.getAttribute(OBJECTMAPPER_ATTRIBUTE);
      try {
        proxyRequest.content(new BytesContentProvider(objectMapper.writeValueAsBytes(query)));
      } catch(JsonProcessingException e) {
        Throwables.propagate(e);
      }
    }
  }

  @Override
  protected Response.Listener newProxyResponseListener(
      HttpServletRequest request, HttpServletResponse response
  )
  {
    final Query query = (Query) request.getAttribute(QUERY_ATTRIBUTE);
    if (query != null) {
      return newMetricsEmittingProxyResponseListener(request, response, query, System.currentTimeMillis());
    } else {
      return super.newProxyResponseListener(request, response);
    }
  }

  @Override
  protected URI rewriteURI(HttpServletRequest request)
  {
    final String host = (String) request.getAttribute(HOST_ATTRIBUTE);
    final StringBuilder uri = new StringBuilder("http://");

    uri.append(host);
    uri.append(request.getRequestURI());
    final String queryString = request.getQueryString();
    if (queryString != null) {
      uri.append("?").append(queryString);
    }
    return URI.create(uri.toString());
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
      long start
  )
  {
    return new MetricsEmittingProxyResponseListener(request, response, query, start);
  }


  private class MetricsEmittingProxyResponseListener extends ProxyResponseListener
  {
    private final HttpServletRequest req;
    private final HttpServletResponse res;
    private final Query query;
    private final long start;

    public MetricsEmittingProxyResponseListener(
        HttpServletRequest request,
        HttpServletResponse response,
        Query query,
        long start
    )
    {
      super(request, response);

      this.req = request;
      this.res = response;
      this.query = query;
      this.start = start;
    }

    @Override
    public void onComplete(Result result)
    {
      final long requestTime = System.currentTimeMillis() - start;
      try {
        emitter.emit(
            QueryMetricUtil.makeRequestTimeMetric(jsonMapper, query, req.getRemoteAddr())
                           .build("request/time", requestTime)
        );

        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "request/time",
                        requestTime,
                        "success",
                        true
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
  }
}
