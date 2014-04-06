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
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import io.druid.client.RoutingDruidClient;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.Query;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.DateTime;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 */
@WebServlet(asyncSupported = true)
public class AsyncQueryForwardingServlet extends HttpServlet
{
  private static final EmittingLogger log = new EmittingLogger(AsyncQueryForwardingServlet.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final String DISPATCHED = "dispatched";

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final RoutingDruidClient routingDruidClient;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final QueryIDProvider idProvider;

  public AsyncQueryForwardingServlet(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      RoutingDruidClient routingDruidClient,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryIDProvider idProvider
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.routingDruidClient = routingDruidClient;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.idProvider = idProvider;
  }

  @Override
  protected void doPost(
      final HttpServletRequest req, final HttpServletResponse resp
  ) throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    String queryId;

    final boolean isSmile = "application/smile".equals(req.getContentType());

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;

    OutputStream out = null;

    try {
      final AsyncContext ctx = req.startAsync(req, resp);

      if (req.getAttribute(DISPATCHED) != null) {
        return;
      }

      req.setAttribute(DISPATCHED, true);

      query = objectMapper.readValue(req.getInputStream(), Query.class);
      queryId = query.getId();
      if (queryId == null) {
        queryId = idProvider.next(query);
        query = query.withId(queryId);
      }

      requestLogger.log(
          new RequestLogLine(new DateTime(), req.getRemoteAddr(), query)
      );
      out = resp.getOutputStream();
      final OutputStream outputStream = out;

      final String host = hostFinder.getHost(query);

      final Query theQuery = query;
      final String theQueryId = queryId;

      final HttpResponseHandler<OutputStream, OutputStream> responseHandler = new HttpResponseHandler<OutputStream, OutputStream>()
      {
        @Override
        public ClientResponse<OutputStream> handleResponse(HttpResponse response)
        {
          resp.setStatus(response.getStatus().getCode());
          resp.setContentType("application/x-javascript");

          byte[] bytes = getContentBytes(response.getContent());
          if (bytes.length > 0) {
            try {
              outputStream.write(bytes);
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
          return ClientResponse.finished(outputStream);
        }

        @Override
        public ClientResponse<OutputStream> handleChunk(
            ClientResponse<OutputStream> clientResponse, HttpChunk chunk
        )
        {
          byte[] bytes = getContentBytes(chunk.getContent());
          if (bytes.length > 0) {
            try {
              clientResponse.getObj().write(bytes);
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
          return clientResponse;
        }

        @Override
        public ClientResponse<OutputStream> done(ClientResponse<OutputStream> clientResponse)
        {
          final long requestTime = System.currentTimeMillis() - start;

          log.info("Request time: %d", requestTime);

          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setUser2(theQuery.getDataSource().getName())
                  .setUser4(theQuery.getType())
                  .setUser5(theQuery.getIntervals().get(0).toString())
                  .setUser6(String.valueOf(theQuery.hasFilters()))
                  .setUser7(req.getRemoteAddr())
                  .setUser8(theQueryId)
                  .setUser9(theQuery.getDuration().toPeriod().toStandardMinutes().toString())
                  .build("request/time", requestTime)
          );

          final OutputStream obj = clientResponse.getObj();
          try {
            resp.flushBuffer();
            outputStream.close();
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
          finally {
            ctx.dispatch();
          }

          return ClientResponse.finished(obj);
        }

        private byte[] getContentBytes(ChannelBuffer content)
        {
          byte[] contentBytes = new byte[content.readableBytes()];
          content.readBytes(contentBytes);
          return contentBytes;
        }
      };

      ctx.start(
          new Runnable()
          {
            @Override
            public void run()
            {
              routingDruidClient.run(host, theQuery, responseHandler);
            }
          }
      );
    }
    catch (Exception e) {
      if (!resp.isCommitted()) {
        resp.setStatus(500);
        resp.resetBuffer();

        if (out == null) {
          out = resp.getOutputStream();
        }

        out.write((e.getMessage() == null) ? "Exception null".getBytes(UTF8) : e.getMessage().getBytes(UTF8));
        out.write("\n".getBytes(UTF8));
      }

      resp.flushBuffer();

      log.makeAlert(e, "Exception handling request")
         .addData("query", query)
         .addData("peer", req.getRemoteAddr())
         .emit();
    }
  }
}
