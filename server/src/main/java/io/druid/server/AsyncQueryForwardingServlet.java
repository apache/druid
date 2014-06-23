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
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
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
    final AsyncContext asyncContext = req.startAsync(req, res);
    asyncContext.start(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              final HttpResponseHandler<OutputStream, OutputStream> responseHandler = new PassthroughHttpResponseHandler(asyncContext, jsonMapper);

              final String host = hostFinder.getDefaultHost();
              routingDruidClient.get(makeUrl(host, (HttpServletRequest) asyncContext.getRequest()), responseHandler);
            }
            catch (Exception e) {
              handleException(jsonMapper, asyncContext, e);
            }
          }
        }
    );
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException
  {
    final AsyncContext asyncContext = req.startAsync(req, res);
    asyncContext.start(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              final HttpResponseHandler<OutputStream, OutputStream> responseHandler = new PassthroughHttpResponseHandler(asyncContext, jsonMapper);

              final String host = hostFinder.getDefaultHost();
              routingDruidClient.delete(makeUrl(host, (HttpServletRequest) asyncContext.getRequest()), responseHandler);
            }
            catch (Exception e) {
              handleException(jsonMapper, asyncContext, e);
            }
          }
        }
    );
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    final AsyncContext asyncContext = req.startAsync(req, res);
    asyncContext.start(
        new Runnable()
        {
          @Override
          public void run()
          {
            final HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();

            final boolean isSmile = QueryResource.APPLICATION_SMILE.equals(request.getContentType());
            final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

            Query inputQuery = null;
            try {
              inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
              if (inputQuery.getId() == null) {
                inputQuery = inputQuery.withId(UUID.randomUUID().toString());
              }
              final Query query = inputQuery;

              if (log.isDebugEnabled()) {
                log.debug("Got query [%s]", inputQuery);
              }

              final HttpResponseHandler<OutputStream, OutputStream> responseHandler = new PassthroughHttpResponseHandler(
                  asyncContext,
                  objectMapper
              )
              {
                @Override
                public ClientResponse<OutputStream> done(ClientResponse<OutputStream> clientResponse)
                {
                  final long requestTime = System.currentTimeMillis() - start;
                  log.debug("Request time: %d", requestTime);

                  emitter.emit(
                      new ServiceMetricEvent.Builder()
                          .setUser2(DataSourceUtil.getMetricName(query.getDataSource()))
                          .setUser4(query.getType())
                          .setUser5(COMMA_JOIN.join(query.getIntervals()))
                          .setUser6(String.valueOf(query.hasFilters()))
                          .setUser7(request.getRemoteAddr())
                          .setUser8(query.getId())
                          .setUser9(query.getDuration().toPeriod().toStandardMinutes().toString())
                          .build("request/time", requestTime)
                  );

                  try {
                    requestLogger.log(
                        new RequestLogLine(
                            new DateTime(),
                            request.getRemoteAddr(),
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
                    throw Throwables.propagate(e);
                  }

                  return super.done(clientResponse);
                }
              };

              routingDruidClient.postQuery(
                  makeUrl(hostFinder.getHost(inputQuery), request),
                  inputQuery,
                  responseHandler
              );
            }
            catch (Exception e) {
              handleException(objectMapper, asyncContext, e);

              try {
                requestLogger.log(
                    new RequestLogLine(
                        new DateTime(),
                        request.getRemoteAddr(),
                        inputQuery,
                        new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", e.toString()))
                    )
                );
              }
              catch (Exception logError) {
                log.error(logError, "Unable to log query [%s]!", inputQuery);
              }

              log.makeAlert(e, "Exception handling request")
                 .addData("query", inputQuery)
                 .addData("peer", request.getRemoteAddr())
                 .emit();
            }
          }
        }
    );
  }

  private String makeUrl(final String host, final HttpServletRequest req)
  {
    final String queryString = req.getQueryString();
    final String requestURI = req.getRequestURI() == null ? "" : req.getRequestURI();

    if (queryString == null) {
      return String.format("http://%s%s", host, requestURI);
    }
    return String.format("http://%s%s?%s", host, requestURI, queryString);
  }

  private static void handleException(ObjectMapper objectMapper, AsyncContext asyncContext, Throwable exception)
  {
    try {
      HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
      if (!response.isCommitted()) {
        final String errorMessage = exception.getMessage() == null ? "null exception" : exception.getMessage();

        response.resetBuffer();
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        objectMapper.writeValue(
            response.getOutputStream(),
            ImmutableMap.of(
                "error", errorMessage
            )
        );
      }
      response.flushBuffer();
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
    finally {
      asyncContext.complete();
    }
  }

  private static class PassthroughHttpResponseHandler implements HttpResponseHandler<OutputStream, OutputStream>
  {
    private final AsyncContext asyncContext;
    private final ObjectMapper objectMapper;
    private final OutputStream outputStream;

    public PassthroughHttpResponseHandler(AsyncContext asyncContext, ObjectMapper objectMapper) throws IOException
    {
      this.asyncContext = asyncContext;
      this.objectMapper = objectMapper;
      this.outputStream = asyncContext.getResponse().getOutputStream();
    }

    protected void copyStatusHeaders(HttpResponse clientResponse)
    {
      final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
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
    public ClientResponse<OutputStream> handleResponse(HttpResponse clientResponse)
    {
      copyStatusHeaders(clientResponse);

      try {
        ChannelBuffer buf = clientResponse.getContent();
        buf.readBytes(outputStream, buf.readableBytes());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      return ClientResponse.finished(outputStream);
    }

    @Override
    public ClientResponse<OutputStream> handleChunk(
        ClientResponse<OutputStream> clientResponse, HttpChunk chunk
    )
    {
      try {
        ChannelBuffer buf = chunk.getContent();
        buf.readBytes(outputStream, buf.readableBytes());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      return clientResponse;
    }

    @Override
    public ClientResponse<OutputStream> done(ClientResponse<OutputStream> clientResponse)
    {
      asyncContext.complete();
      return ClientResponse.finished(clientResponse.getObj());
    }

    @Override
    public void exceptionCaught(
        ClientResponse<OutputStream> clientResponse,
        Throwable e
    )
    {
      // throwing an exception here may cause resource leak
      try {
        handleException(objectMapper, asyncContext, e);
      } catch(Exception err) {
        log.error(err, "Unable to handle exception response");
      }
    }
  }
}
