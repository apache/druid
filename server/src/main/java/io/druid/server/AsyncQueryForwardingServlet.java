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
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.client.RoutingDruidClient;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.DataSourceUtil;
import io.druid.query.Query;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
@WebServlet(asyncSupported = true)
public class AsyncQueryForwardingServlet extends HttpServlet
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  private static final Joiner COMMA_JOIN = Joiner.on(",");

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final RoutingDruidClient routingDruidClient;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  public AsyncQueryForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      RoutingDruidClient routingDruidClient,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.routingDruidClient = routingDruidClient;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException
  {
    final AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(0);

    final HttpResponseHandler<ServletOutputStream, ServletOutputStream> responseHandler =
        new PassthroughHttpResponseHandler(res);

    final URI uri = rewriteURI(hostFinder.getDefaultHost(), req);
    asyncComplete(
        res,
        asyncContext,
        jsonMapper,
        routingDruidClient.get(uri, responseHandler)
    );
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException
  {
    final AsyncContext asyncContext = req.startAsync();
    asyncContext.setTimeout(0);

    final HttpResponseHandler<ServletOutputStream, ServletOutputStream> responseHandler =
        new PassthroughHttpResponseHandler(res);

    final String host = hostFinder.getDefaultHost();

    asyncComplete(
        res,
        asyncContext,
        jsonMapper,
        routingDruidClient.delete(rewriteURI(host, (HttpServletRequest) asyncContext.getRequest()), responseHandler)
    );
  }

  @Override
  protected void doPost(final HttpServletRequest req, final HttpServletResponse res) throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    final boolean isSmile = QueryResource.APPLICATION_SMILE.equals(req.getContentType());
    final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

    try {
      final Query inputQuery = objectMapper.readValue(req.getInputStream(), Query.class);
      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", inputQuery);
      }

      final Query query;
      if (inputQuery.getId() == null) {
        query = inputQuery.withId(UUID.randomUUID().toString());
      } else {
        query = inputQuery;
      }

      URI rewrittenURI = rewriteURI(hostFinder.getHost(query), req);

      final AsyncContext asyncContext = req.startAsync();
      // let proxy http client timeout
      asyncContext.setTimeout(0);

      ListenableFuture future = routingDruidClient.postQuery(
          rewrittenURI,
          query,
          new PassthroughHttpResponseHandler(res)
      );

      Futures.addCallback(
          future,
          new FutureCallback()
          {
            @Override
            public void onSuccess(@Nullable Object o)
            {
              final long requestTime = System.currentTimeMillis() - start;
              emitter.emit(
                  new ServiceMetricEvent.Builder()
                      .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
                      .setUser3(String.valueOf(query.getContextPriority(0)))
                      .setUser4(query.getType())
                      .setUser5(COMMA_JOIN.join(query.getIntervals()))
                      .setUser6(String.valueOf(query.hasFilters()))
                      .setUser7(req.getRemoteAddr())
                      .setUser8(query.getId())
                      .setUser9(query.getDuration().toPeriod().toStandardMinutes().toString())
                      .build("request/time", requestTime)
              );

              try {
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
            }

            @Override
            public void onFailure(Throwable throwable)
            {
              try {
                final String errorMessage = throwable.getMessage();
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
                                errorMessage == null ? "no message" : errorMessage)
                        )
                    )
                );
              }
              catch (IOException logError) {
                log.error(logError, "Unable to log query [%s]!", query);
              }

              log.makeAlert(throwable, "Exception handling request [%s]", query.getId())
                 .addData("query", query)
                 .addData("peer", req.getRemoteAddr())
                 .emit();
            }
          }
      );

      asyncComplete(
          res,
          asyncContext,
          objectMapper,
          future
      );
    } catch(IOException e) {
      log.warn(e, "Exception parsing query");
      final String errorMessage = e.getMessage() == null ? "no error message" : e.getMessage();
      requestLogger.log(
          new RequestLogLine(
              new DateTime(),
              req.getRemoteAddr(),
              null,
              new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", errorMessage))
          )
      );
      res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      objectMapper.writeValue(
          res.getOutputStream(),
          ImmutableMap.of("error", errorMessage)
      );
    } catch(Exception e) {
      handleException(res, objectMapper, e);
    }
  }

  private static void asyncComplete(
      final HttpServletResponse res,
      final AsyncContext asyncContext,
      final ObjectMapper objectMapper,
      ListenableFuture future
  )
  {
    Futures.addCallback(
        future,
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(@Nullable Object o)
          {
            asyncContext.complete();
          }

          @Override
          public void onFailure(Throwable throwable)
          {
            log.error(throwable, "Error processing query response");
            try {
              handleException(res, objectMapper, throwable);
            } catch(Exception err) {
              log.error(err, "Unable to handle exception response");
            }
            asyncContext.complete();
          }
        }
    );
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

  private static void handleException(HttpServletResponse response, ObjectMapper objectMapper, Throwable exception) throws IOException
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

  private static class PassthroughHttpResponseHandler implements HttpResponseHandler<ServletOutputStream, ServletOutputStream>
  {
    private final HttpServletResponse response;

    public PassthroughHttpResponseHandler(HttpServletResponse response) throws IOException
    {
      this.response = response;
    }

    protected void copyStatusHeaders(HttpResponse clientResponse)
    {
      response.setStatus(clientResponse.getStatus().getCode());
      response.setContentType(clientResponse.headers().get(HttpHeaders.Names.CONTENT_TYPE));

      FluentIterable.from(clientResponse.headers().entries())
          .filter(new Predicate<Map.Entry<String, String>>()
              {
                @Override
                public boolean apply(@Nullable Map.Entry<String, String> input)
                {
                  return input.getKey().startsWith("X-Druid");
                }
              }
          )
          .transform(
              new Function<Map.Entry<String, String>, Object>()
              {
                @Nullable
                @Override
                public Object apply(@Nullable Map.Entry<String, String> input)
                {
                  response.setHeader(input.getKey(), input.getValue());
                  return null;
                }
              }
          )
          .allMatch(Predicates.alwaysTrue());
    }

    @Override
    public ClientResponse<ServletOutputStream> handleResponse(HttpResponse clientResponse)
    {
      copyStatusHeaders(clientResponse);

      try {
        final ServletOutputStream outputStream = response.getOutputStream();
        ChannelBuffer buf = clientResponse.getContent();
        buf.readBytes(outputStream, buf.readableBytes());
        return ClientResponse.unfinished(outputStream);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public ClientResponse<ServletOutputStream> handleChunk(
        ClientResponse<ServletOutputStream> clientResponse, HttpChunk chunk
    )
    {
      try {
        ChannelBuffer buf = chunk.getContent();
        buf.readBytes(clientResponse.getObj(), buf.readableBytes());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return clientResponse;
    }

    @Override
    public ClientResponse<ServletOutputStream> done(ClientResponse<ServletOutputStream> clientResponse)
    {
      return ClientResponse.finished(clientResponse.getObj());
    }

    @Override
    public void exceptionCaught(
        ClientResponse<ServletOutputStream> clientResponse,
        Throwable e
    )
    {
      // exceptions are handled on future callback
    }
  }
}
