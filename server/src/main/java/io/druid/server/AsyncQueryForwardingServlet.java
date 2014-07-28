/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.Query;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.joda.time.DateTime;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);

  private static void handleException(HttpServletResponse response, ObjectMapper objectMapper, Throwable exception)
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
  private final RequestLogger requestLogger;

  public AsyncQueryForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router HttpClient httpClient,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClient = httpClient;
    this.requestLogger = requestLogger;
  }

  @Override
  protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
  {
    final boolean isSmile = QueryResource.APPLICATION_SMILE.equals(request.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

    String host = hostFinder.getDefaultHost();
    Query inputQuery = null;
    try {
      inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
      if (inputQuery != null) {
        host = hostFinder.getHost(inputQuery);
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
      objectMapper.writeValue(
          response.getOutputStream(),
          ImmutableMap.of("error", errorMessage)
      );
    }
    catch (Exception e) {
      handleException(response, objectMapper, e);
    }

    final int requestId = getRequestId(request);

    URI rewrittenURI = rewriteURI(host, request);

    if (_log.isDebugEnabled()) {
      StringBuffer uri = request.getRequestURL();
      if (request.getQueryString() != null) {
        uri.append("?").append(request.getQueryString());
      }
      if (_log.isDebugEnabled()) {
        _log.debug("{} rewriting: {} -> {}", requestId, uri, rewrittenURI);
      }
    }

    if (rewrittenURI == null) {
      onRewriteFailed(request, response);
      return;
    }

    final Request proxyRequest = getHttpClient().newRequest(rewrittenURI)
                                                .method(request.getMethod())
                                                .version(HttpVersion.fromString(request.getProtocol()));

    // Copy headers
    boolean hasContent = request.getContentLength() > 0 || request.getContentType() != null;
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

    if (hasContent && inputQuery != null) {
      proxyRequest.content(new BytesContentProvider(jsonMapper.writeValueAsBytes(inputQuery)));
    }

    customizeProxyRequest(proxyRequest, request);

    if (_log.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder(request.getMethod());
      builder.append(" ").append(request.getRequestURI());
      String query = request.getQueryString();
      if (query != null) {
        builder.append("?").append(query);
      }
      builder.append(" ").append(request.getProtocol()).append("\r\n");
      for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements(); ) {
        String headerName = headerNames.nextElement();
        builder.append(headerName).append(": ");
        for (Enumeration<String> headerValues = request.getHeaders(headerName); headerValues.hasMoreElements(); ) {
          String headerValue = headerValues.nextElement();
          if (headerValue != null) {
            builder.append(headerValue);
          }
          if (headerValues.hasMoreElements()) {
            builder.append(",");
          }
        }
        builder.append("\r\n");
      }
      builder.append("\r\n");

      _log.debug(
          "{} proxying to upstream:{}{}{}{}",
          requestId,
          System.lineSeparator(),
          builder,
          proxyRequest,
          System.lineSeparator(),
          proxyRequest.getHeaders().toString().trim()
      );
    }

    proxyRequest.send(newProxyResponseListener(request, response));
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
}
