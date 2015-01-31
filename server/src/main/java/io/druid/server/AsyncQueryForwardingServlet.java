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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.Query;
import io.druid.query.QueryMetricUtil;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.joda.time.DateTime;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
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
  private final HttpClient httpClient;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  public AsyncQueryForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router HttpClient httpClient,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClient = httpClient;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType()) || APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

    String host = hostFinder.getDefaultHost();
    Query inputQuery = null;
    boolean hasContent = request.getContentLength() > 0 || request.getContentType() != null;
    boolean isQuery = request.getMethod().equals(HttpMethod.POST.asString());
    long startTime = System.currentTimeMillis();

    // queries only exist for POST
    if (isQuery) {
      try {
        inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        if (inputQuery != null) {
          host = hostFinder.getHost(inputQuery);
          if (inputQuery.getId() == null) {
            inputQuery = inputQuery.withId(UUID.randomUUID().toString());
          }
        }
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

    URI rewrittenURI = rewriteURI(host, request);
    if (rewrittenURI == null) {
      onRewriteFailed(request, response);
      return;
    }

    final Request proxyRequest = getHttpClient().newRequest(rewrittenURI)
                                                .method(request.getMethod())
                                                .version(HttpVersion.fromString(request.getProtocol()));

    // Copy headers
    for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements(); ) {
      String headerName = headerNames.nextElement();

      if (HttpHeader.TRANSFER_ENCODING.is(headerName)) {
        hasContent = true;
      }

      for (Enumeration<String> headerValues = request.getHeaders(headerName); headerValues.hasMoreElements(); ) {
        String headerValue = headerValues.nextElement();
        if (headerValue != null) {
          proxyRequest.header(headerName, headerValue);
        }
      }
    }

    // Add proxy headers
    addViaHeader(proxyRequest);

    addXForwardedHeaders(proxyRequest, request);

    final AsyncContext asyncContext = request.startAsync();
    // We do not timeout the continuation, but the proxy request
    asyncContext.setTimeout(0);
    proxyRequest.timeout(
        getTimeout(), TimeUnit.MILLISECONDS
    );

    if (hasContent) {
      if (inputQuery != null) {
        proxyRequest.content(new BytesContentProvider(jsonMapper.writeValueAsBytes(inputQuery)));
      } else {
        proxyRequest.content(proxyRequestContent(proxyRequest, request));
      }
    }

    customizeProxyRequest(proxyRequest, request);

    if (isQuery) {
      proxyRequest.send(newMetricsEmittingProxyResponseListener(request, response, inputQuery, startTime));
    } else {
      proxyRequest.send(newProxyResponseListener(request, response));
    }
  }

  @Override
  protected HttpClient createHttpClient() throws ServletException
  {
    return httpClient;
  }

  private URI rewriteURI(final String host, final HttpServletRequest req)
  {
    final StringBuilder uri = new StringBuilder("http://");

    uri.append(host);
    uri.append(req.getRequestURI());
    final String queryString = req.getQueryString();
    if (queryString != null) {
      uri.append("?").append(queryString);
    }
    return URI.create(uri.toString());
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
