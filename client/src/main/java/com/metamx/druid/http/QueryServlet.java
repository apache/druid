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

package com.metamx.druid.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.druid.Query;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import org.joda.time.DateTime;
import org.mortbay.jetty.Request;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 */
public class QueryServlet extends HttpServlet
{
  private static final Logger log = new Logger(QueryServlet.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QuerySegmentWalker texasRanger;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  public QueryServlet(
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.texasRanger = texasRanger;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
  {
    Query query = null;
    byte[] requestQuery = null;

    final boolean isSmile = "application/smile".equals(req.getContentType());

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    ObjectWriter jsonWriter = req.getParameter("pretty") == null
                              ? objectMapper.writer()
                              : objectMapper.writerWithDefaultPrettyPrinter();
    OutputStream out = null;

    try {
      requestQuery = ByteStreams.toByteArray(req.getInputStream());
      query = objectMapper.readValue(requestQuery, Query.class);

      requestLogger.log(
          new RequestLogLine(new DateTime(), req.getRemoteAddr(), query)
      );

      Sequence<?> results = query.run(texasRanger);

      if (results == null) {
        results = Sequences.empty();
      }

      resp.setStatus(200);
      resp.setContentType("application/x-javascript");

      out = resp.getOutputStream();
      jsonWriter.writeValue(out, results);

      long requestTime = System.currentTimeMillis() - ((Request) req).getTimeStamp();

      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser2(query.getDataSource())
                  //.setUser3(originatorType)
              .setUser4(query.getType())
              .setUser5(query.getIntervals().get(0).toString())
              .setUser6(String.valueOf(query.hasFilters()))
                  //.setUser8(originatorId)
              .setUser9(query.getDuration().toPeriod().toStandardMinutes().toString())
              .build("request/time", requestTime)
      );
    }
    catch (Exception e) {
      final String queryString =
          query == null
          ? (isSmile ? "smile_unknown" : new String(requestQuery, Charsets.UTF_8))
          : query.toString();

      log.warn(e, "Exception occurred on request [%s]", queryString);

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

      emitter.emit(
          new AlertEvent.Builder().build(
              "Exception handling request",
              ImmutableMap.<String, Object>builder()
                          .put("exception", e.toString())
                          .put("query", queryString)
                          .put("peer", req.getRemoteAddr())
                          .build()
          )
      );
    }
    finally {
      resp.flushBuffer();
      Closeables.closeQuietly(out);
    }
  }
}
