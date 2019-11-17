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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  public static final String QUERY_FAIL_TIME = "queryFailTime";

  private static final Logger log = new Logger(DirectDruidClient.class);

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String scheme;
  private final String host;
  private final ServiceEmitter emitter;

  private final AtomicInteger openConnections;
  private final boolean isSmile;
  private final ScheduledExecutorService queryCancellationExecutor;

  /**
   * Removes the magical fields added by {@link #makeResponseContextForQuery()}.
   */
  public static void removeMagicResponseContextFields(ResponseContext responseContext)
  {
    responseContext.remove(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED);
  }

  public static ResponseContext makeResponseContextForQuery()
  {
    final ResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.put(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED, new AtomicLong());
    return responseContext;
  }

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String scheme,
      String host,
      ServiceEmitter emitter
  )
  {
    this.warehouse = warehouse;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.scheme = scheme;
    this.host = host;
    this.emitter = emitter;

    this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    this.openConnections = new AtomicInteger();
    this.queryCancellationExecutor = Execs.scheduledSingleThreaded("query-cancellation-executor");
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    final Query<T> query = queryPlus.getQuery();
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    boolean isBySegment = QueryContexts.isBySegment(query);
    final JavaType queryResultType = isBySegment ? toolChest.getBySegmentResultType() : toolChest.getBaseResultType();

    final ListenableFuture<InputStream> future;
    final String url = StringUtils.format("%s://%s/druid/v2/", scheme, host);
    final String cancelUrl = StringUtils.format("%s://%s/druid/v2/%s", scheme, host, query.getId());

    try {
      log.debug("Querying queryId[%s] url[%s]", query.getId(), url);

      final long timeoutAt = query.getContextValue(QUERY_FAIL_TIME);
      final HttpResponseHandler<InputStream, InputStream> responseHandler = new QueryContextInputStreamResponseHandler(query, context, warehouse, url, timeoutAt, objectMapper);

      long timeLeft = timeoutAt - System.currentTimeMillis();

      if (timeLeft <= 0) {
        throw new RE("Query[%s] url[%s] timed out.", query.getId(), url);
      }

      future = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(url)
          ).setContent(objectMapper.writeValueAsBytes(QueryContexts.withTimeout(query, timeLeft)))
           .setHeader(
               HttpHeaders.Names.CONTENT_TYPE,
               isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON
           ),
          responseHandler,
          Duration.millis(timeLeft)
      );

      queryWatcher.registerQuery(query, future);

      openConnections.getAndIncrement();
      Futures.addCallback(
          future,
          new FutureCallback<InputStream>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
              if (future.isCancelled()) {
                cancelQuery(query, cancelUrl);
              }
            }
          },
          // The callback is non-blocking and quick, so it's OK to schedule it using directExecutor()
          Execs.directExecutor()
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<T>(
                queryResultType,
                future,
                url,
                query,
                host,
                toolChest.decorateObjectMapper(objectMapper, query),
                null
            );
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            CloseQuietly.close(iterFromMake);
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makePreComputeManipulatorFn(
              query,
              MetricManipulatorFns.deserializing()
          )
      );
    }

    return retVal;
  }

  private <T> void cancelQuery(Query<T> query, String cancelUrl)
  {
    Runnable cancelRunnable = () -> {
      try {
        Future<StatusResponseHolder> responseFuture = httpClient.go(
            new Request(HttpMethod.DELETE, new URL(cancelUrl))
            .setContent(objectMapper.writeValueAsBytes(query))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON),
            StatusResponseHandler.getInstance(),
            Duration.standardSeconds(1));

        Runnable checkRunnable = () -> {
          try {
            if (!responseFuture.isDone()) {
              log.error("Error cancelling query[%s]", query);
            }
            StatusResponseHolder response = responseFuture.get();
            if (response.getStatus().getCode() >= 500) {
              log.error("Error cancelling query[%s]: queriable node returned status[%d] [%s].",
                  query,
                  response.getStatus().getCode(),
                  response.getStatus().getReasonPhrase());
            }
          }
          catch (ExecutionException | InterruptedException e) {
            log.error(e, "Error cancelling query[%s]", query);
          }
        };
        queryCancellationExecutor.schedule(checkRunnable, 5, TimeUnit.SECONDS);
      }
      catch (IOException e) {
        log.error(e, "Error cancelling query[%s]", query);
      }
    };
    queryCancellationExecutor.submit(cancelRunnable);
  }

  @Override
  public String toString()
  {
    return "DirectDruidClient{" +
           "host='" + host + '\'' +
           ", isSmile=" + isSmile +
           '}';
  }
}
