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

package io.druid.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.ClientResponse;
import io.druid.java.util.http.client.response.HttpResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryWatcher;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulatorFns;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  public static final String QUERY_FAIL_TIME = "queryFailTime";
  public static final String QUERY_TOTAL_BYTES_GATHERED = "queryTotalBytesGathered";

  private static final Logger log = new Logger(DirectDruidClient.class);

  private static final Map<Class<? extends Query>, Pair<JavaType, JavaType>> typesMap = new ConcurrentHashMap<>();

  private final QueryToolChestWarehouse warehouse;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String scheme;
  private final String host;
  private final ServiceEmitter emitter;

  private final AtomicInteger openConnections;
  private final boolean isSmile;

  /**
   * Removes the magical fields added by {@link #makeResponseContextForQuery()}.
   */
  public static void removeMagicResponseContextFields(Map<String, Object> responseContext)
  {
    responseContext.remove(DirectDruidClient.QUERY_TOTAL_BYTES_GATHERED);
  }

  public static Map<String, Object> makeResponseContextForQuery()
  {
    final Map<String, Object> responseContext = new ConcurrentHashMap<>();
    responseContext.put(
        DirectDruidClient.QUERY_TOTAL_BYTES_GATHERED,
        new AtomicLong()
    );
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
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> context)
  {
    final Query<T> query = queryPlus.getQuery();
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    boolean isBySegment = QueryContexts.isBySegment(query);

    Pair<JavaType, JavaType> types = typesMap.get(query.getClass());
    if (types == null) {
      final TypeFactory typeFactory = objectMapper.getTypeFactory();
      JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference());
      JavaType bySegmentType = typeFactory.constructParametricType(
          Result.class, typeFactory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
      types = Pair.of(baseType, bySegmentType);
      typesMap.put(query.getClass(), types);
    }

    final JavaType typeRef;
    if (isBySegment) {
      typeRef = types.rhs;
    } else {
      typeRef = types.lhs;
    }

    final ListenableFuture<InputStream> future;
    final String url = StringUtils.format("%s://%s/druid/v2/", scheme, host);
    final String cancelUrl = StringUtils.format("%s://%s/druid/v2/%s", scheme, host, query.getId());

    try {
      log.debug("Querying queryId[%s] url[%s]", query.getId(), url);

      final long requestStartTimeNs = System.nanoTime();

      long timeoutAt = query.getContextValue(QUERY_FAIL_TIME);
      long maxScatterGatherBytes = QueryContexts.getMaxScatterGatherBytes(query);
      AtomicLong totalBytesGathered = (AtomicLong) context.get(QUERY_TOTAL_BYTES_GATHERED);

      final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<InputStream, InputStream>()
      {
        private final AtomicLong byteCount = new AtomicLong(0);
        private final BlockingQueue<InputStream> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<String> fail = new AtomicReference<>();

        private QueryMetrics<? super Query<T>> queryMetrics;
        private long responseStartTimeNs;

        private QueryMetrics<? super Query<T>> acquireResponseMetrics()
        {
          if (queryMetrics == null) {
            queryMetrics = toolChest.makeMetrics(query);
            queryMetrics.server(host);
          }
          return queryMetrics;
        }

        @Override
        public ClientResponse<InputStream> handleResponse(HttpResponse response)
        {
          checkQueryTimeout();
          checkTotalBytesLimit(response.getContent().readableBytes());

          log.debug("Initial response from url[%s] for queryId[%s]", url, query.getId());
          responseStartTimeNs = System.nanoTime();
          acquireResponseMetrics().reportNodeTimeToFirstByte(responseStartTimeNs - requestStartTimeNs).emit(emitter);

          try {
            final String responseContext = response.headers().get("X-Druid-Response-Context");
            // context may be null in case of error or query timeout
            if (responseContext != null) {
              context.putAll(
                  objectMapper.<Map<String, Object>>readValue(
                      responseContext, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
                  )
              );
            }
            queue.put(new ChannelBufferInputStream(response.getContent()));
          }
          catch (final IOException e) {
            log.error(e, "Error parsing response context from url [%s]", url);
            return ClientResponse.<InputStream>finished(
                new InputStream()
                {
                  @Override
                  public int read() throws IOException
                  {
                    throw e;
                  }
                }
            );
          }
          catch (InterruptedException e) {
            log.error(e, "Queue appending interrupted");
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
          }
          byteCount.addAndGet(response.getContent().readableBytes());
          return ClientResponse.<InputStream>finished(
              new SequenceInputStream(
                  new Enumeration<InputStream>()
                  {
                    @Override
                    public boolean hasMoreElements()
                    {
                      if (fail.get() != null) {
                        throw new RE(fail.get());
                      }
                      checkQueryTimeout();

                      // Done is always true until the last stream has be put in the queue.
                      // Then the stream should be spouting good InputStreams.
                      synchronized (done) {
                        return !done.get() || !queue.isEmpty();
                      }
                    }

                    @Override
                    public InputStream nextElement()
                    {
                      if (fail.get() != null) {
                        throw new RE(fail.get());
                      }

                      try {
                        InputStream is = queue.poll(checkQueryTimeout(), TimeUnit.MILLISECONDS);
                        if (is != null) {
                          return is;
                        } else {
                          throw new RE("Query[%s] url[%s] timed out.", query.getId(), url);
                        }
                      }
                      catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                      }
                    }
                  }
              )
          );
        }

        @Override
        public ClientResponse<InputStream> handleChunk(
            ClientResponse<InputStream> clientResponse, HttpChunk chunk
        )
        {
          checkQueryTimeout();

          final ChannelBuffer channelBuffer = chunk.getContent();
          final int bytes = channelBuffer.readableBytes();

          checkTotalBytesLimit(bytes);

          if (bytes > 0) {
            try {
              queue.put(new ChannelBufferInputStream(channelBuffer));
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            byteCount.addAndGet(bytes);
          }
          return clientResponse;
        }

        @Override
        public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
        {
          long stopTimeNs = System.nanoTime();
          long nodeTimeNs = stopTimeNs - requestStartTimeNs;
          final long nodeTimeMs = TimeUnit.NANOSECONDS.toMillis(nodeTimeNs);
          log.debug(
              "Completed queryId[%s] request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
              query.getId(),
              url,
              byteCount.get(),
              nodeTimeMs,
              byteCount.get() / (0.001 * nodeTimeMs) // Floating math; division by zero will yield Inf, not exception
          );
          QueryMetrics<? super Query<T>> responseMetrics = acquireResponseMetrics();
          responseMetrics.reportNodeTime(nodeTimeNs);
          responseMetrics.reportNodeBytes(byteCount.get());
          responseMetrics.emit(emitter);
          synchronized (done) {
            try {
              // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
              // after done is set to true, regardless of the rest of the stream's state.
              queue.put(ByteSource.empty().openStream());
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw Throwables.propagate(e);
            }
            catch (IOException e) {
              // This should never happen
              throw Throwables.propagate(e);
            }
            finally {
              done.set(true);
            }
          }
          return ClientResponse.<InputStream>finished(clientResponse.getObj());
        }

        @Override
        public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
        {
          String msg = StringUtils.format(
              "Query[%s] url[%s] failed with exception msg [%s]",
              query.getId(),
              url,
              e.getMessage()
          );
          setupResponseReadFailure(msg, e);
        }

        private void setupResponseReadFailure(String msg, Throwable th)
        {
          fail.set(msg);
          queue.clear();
          queue.offer(new InputStream()
          {
            @Override
            public int read() throws IOException
            {
              if (th != null) {
                throw new IOException(msg, th);
              } else {
                throw new IOException(msg);
              }
            }
          });

        }

        // Returns remaining timeout or throws exception if timeout already elapsed.
        private long checkQueryTimeout()
        {
          long timeLeft = timeoutAt - System.currentTimeMillis();
          if (timeLeft <= 0) {
            String msg = StringUtils.format("Query[%s] url[%s] timed out.", query.getId(), url);
            setupResponseReadFailure(msg, null);
            throw new RE(msg);
          } else {
            return timeLeft;
          }
        }

        private void checkTotalBytesLimit(long bytes)
        {
          if (maxScatterGatherBytes < Long.MAX_VALUE && totalBytesGathered.addAndGet(bytes) > maxScatterGatherBytes) {
            String msg = StringUtils.format(
                "Query[%s] url[%s] max scatter-gather bytes limit reached.",
                query.getId(),
                url
            );
            setupResponseReadFailure(msg, null);
            throw new RE(msg);
          }
        }
      };

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
          future, new FutureCallback<InputStream>()
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
                // forward the cancellation to underlying queriable node
                try {
                  StatusResponseHolder res = httpClient.go(
                      new Request(
                          HttpMethod.DELETE,
                          new URL(cancelUrl)
                      ).setContent(objectMapper.writeValueAsBytes(query))
                       .setHeader(
                           HttpHeaders.Names.CONTENT_TYPE,
                           isSmile
                           ? SmileMediaTypes.APPLICATION_JACKSON_SMILE
                           : MediaType.APPLICATION_JSON
                       ),
                      new StatusResponseHandler(Charsets.UTF_8),
                      Duration.standardSeconds(1)
                  ).get(1, TimeUnit.SECONDS);

                  if (res.getStatus().getCode() >= 500) {
                    throw new RE(
                        "Error cancelling query[%s]: queriable node returned status[%d] [%s].",
                        res.getStatus().getCode(),
                        res.getStatus().getReasonPhrase()
                    );
                  }
                }
                catch (IOException | ExecutionException | InterruptedException | TimeoutException e) {
                  Throwables.propagate(e);
                }
              }
            }
          }
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<T>(typeRef, future, url, query);
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

  private class JsonParserIterator<T> implements Iterator<T>, Closeable
  {
    private JsonParser jp;
    private ObjectCodec objectCodec;
    private final JavaType typeRef;
    private final Future<InputStream> future;
    private final Query<T> query;
    private final String url;

    public JsonParserIterator(JavaType typeRef, Future<InputStream> future, String url, Query<T> query)
    {
      this.typeRef = typeRef;
      this.future = future;
      this.url = url;
      this.query = query;
      jp = null;
    }

    @Override
    public boolean hasNext()
    {
      init();

      if (jp.isClosed()) {
        return false;
      }
      if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
        CloseQuietly.close(jp);
        return false;
      }

      return true;
    }

    @Override
    public T next()
    {
      init();

      try {
        final T retVal = objectCodec.readValue(jp, typeRef);
        jp.nextToken();
        return retVal;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    private void init()
    {
      if (jp == null) {
        try {
          InputStream is = future.get();
          if (is == null) {
            throw new QueryInterruptedException(
                new ResourceLimitExceededException(
                    "query[%s] url[%s] timed out or max bytes limit reached.",
                    query.getId(),
                    url
                ),
                host
            );
          } else {
            jp = objectMapper.getFactory().createParser(is);
          }
          final JsonToken nextToken = jp.nextToken();
          if (nextToken == JsonToken.START_OBJECT) {
            QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
            throw new QueryInterruptedException(cause, host);
          } else if (nextToken != JsonToken.START_ARRAY) {
            throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
          } else {
            jp.nextToken();
            objectCodec = jp.getCodec();
          }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
          throw new RE(
              e,
              "Failure getting results for query[%s] url[%s] because of [%s]",
              query.getId(),
              url,
              e.getMessage()
          );
        }
        catch (CancellationException e) {
          throw new QueryInterruptedException(e, host);
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      if (jp != null) {
        jp.close();
      }
    }
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
