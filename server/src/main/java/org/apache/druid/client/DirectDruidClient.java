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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResource;
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
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

  private static final Logger log = new Logger(DirectDruidClient.class);
  private static final int VAL_TO_REDUCE_REMAINING_RESPONSES = -1;

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
    responseContext.remove(ResponseContext.Keys.QUERY_TOTAL_BYTES_GATHERED);
    responseContext.remove(ResponseContext.Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS);
  }

  public static ConcurrentResponseContext makeResponseContextForQuery()
  {
    final ConcurrentResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.initialize();
    return responseContext;
  }

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String scheme,
      String host,
      ServiceEmitter emitter,
      ScheduledExecutorService queryCancellationExecutor
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
    this.queryCancellationExecutor = queryCancellationExecutor;
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
    boolean isBySegment = query.context().isBySegment();
    final JavaType queryResultType = isBySegment ? toolChest.getBySegmentResultType() : toolChest.getBaseResultType();

    final ListenableFuture<InputStream> future;
    final String url = scheme + "://" + host + "/druid/v2/";
    final String cancelUrl = url + query.getId();

    try {
      log.debug("Querying queryId [%s] url [%s]", query.getId(), url);

      final long requestStartTimeNs = System.nanoTime();
      final QueryContext queryContext = query.context();
      // Will NPE if the value is not set.
      final long timeoutAt = queryContext.getLong(QUERY_FAIL_TIME);
      final long maxScatterGatherBytes = queryContext.getMaxScatterGatherBytes();
      final AtomicLong totalBytesGathered = context.getTotalBytes();
      final long maxQueuedBytes = queryContext.getMaxQueuedBytes(0);
      final boolean usingBackpressure = maxQueuedBytes > 0;

      final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<InputStream, InputStream>()
      {
        private final AtomicLong totalByteCount = new AtomicLong(0);
        private final AtomicLong queuedByteCount = new AtomicLong(0);
        private final AtomicLong channelSuspendedTime = new AtomicLong(0);
        private final BlockingQueue<InputStreamHolder> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<String> fail = new AtomicReference<>();
        private final AtomicReference<TrafficCop> trafficCopRef = new AtomicReference<>();

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

        /**
         * Queue a buffer. Returns true if we should keep reading, false otherwise.
         */
        private boolean enqueue(ChannelBuffer buffer, long chunkNum) throws InterruptedException
        {
          // Increment queuedByteCount before queueing the object, so queuedByteCount is at least as high as
          // the actual number of queued bytes at any particular time.
          final InputStreamHolder holder = InputStreamHolder.fromChannelBuffer(buffer, chunkNum);
          final long currentQueuedByteCount = queuedByteCount.addAndGet(holder.getLength());
          queue.put(holder);

          // True if we should keep reading.
          return !usingBackpressure || currentQueuedByteCount < maxQueuedBytes;
        }

        private InputStream dequeue() throws InterruptedException
        {
          final InputStreamHolder holder = queue.poll(checkQueryTimeout(), TimeUnit.MILLISECONDS);
          if (holder == null) {
            throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query[%s] url[%s] timed out.", query.getId(), url));
          }

          final long currentQueuedByteCount = queuedByteCount.addAndGet(-holder.getLength());
          if (usingBackpressure && currentQueuedByteCount < maxQueuedBytes) {
            long backPressureTime = Preconditions.checkNotNull(trafficCopRef.get(), "No TrafficCop, how can this be?")
                                                 .resume(holder.getChunkNum());
            channelSuspendedTime.addAndGet(backPressureTime);
          }

          return holder.getStream();
        }

        @Override
        public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
        {
          trafficCopRef.set(trafficCop);
          checkQueryTimeout();
          checkTotalBytesLimit(response.getContent().readableBytes());

          log.debug("Initial response from url[%s] for queryId[%s]", url, query.getId());
          responseStartTimeNs = System.nanoTime();
          acquireResponseMetrics().reportNodeTimeToFirstByte(responseStartTimeNs - requestStartTimeNs).emit(emitter);

          final boolean continueReading;
          try {
            log.trace(
                "Got a response from [%s] for query ID[%s], subquery ID[%s]",
                url,
                query.getId(),
                query.getSubQueryId()
            );
            final String responseContext = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
            context.addRemainingResponse(query.getMostSpecificId(), VAL_TO_REDUCE_REMAINING_RESPONSES);
            // context may be null in case of error or query timeout
            if (responseContext != null) {
              context.merge(ResponseContext.deserialize(responseContext, objectMapper));
            }
            continueReading = enqueue(response.getContent(), 0L);
          }
          catch (final IOException e) {
            log.error(e, "Error parsing response context from url [%s]", url);
            return ClientResponse.finished(
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
            throw new RuntimeException(e);
          }
          totalByteCount.addAndGet(response.getContent().readableBytes());
          return ClientResponse.finished(
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
                        return dequeue();
                      }
                      catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    }
                  }
              ),
              continueReading
          );
        }

        @Override
        public ClientResponse<InputStream> handleChunk(
            ClientResponse<InputStream> clientResponse,
            HttpChunk chunk,
            long chunkNum
        )
        {
          checkQueryTimeout();

          final ChannelBuffer channelBuffer = chunk.getContent();
          final int bytes = channelBuffer.readableBytes();

          checkTotalBytesLimit(bytes);

          boolean continueReading = true;
          if (bytes > 0) {
            try {
              continueReading = enqueue(channelBuffer, chunkNum);
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            totalByteCount.addAndGet(bytes);
          }

          return ClientResponse.finished(clientResponse.getObj(), continueReading);
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
              totalByteCount.get(),
              nodeTimeMs,
              // Floating math; division by zero will yield Inf, not exception
              totalByteCount.get() / (0.001 * nodeTimeMs)
          );
          QueryMetrics<? super Query<T>> responseMetrics = acquireResponseMetrics();
          responseMetrics.reportNodeTime(nodeTimeNs);
          responseMetrics.reportNodeBytes(totalByteCount.get());

          if (usingBackpressure) {
            responseMetrics.reportBackPressureTime(channelSuspendedTime.get());
          }

          responseMetrics.emit(emitter);
          synchronized (done) {
            try {
              // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
              // after done is set to true, regardless of the rest of the stream's state.
              queue.put(InputStreamHolder.fromChannelBuffer(ChannelBuffers.EMPTY_BUFFER, Long.MAX_VALUE));
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            finally {
              done.set(true);
            }
          }
          return ClientResponse.finished(clientResponse.getObj());
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
          queue.offer(
              InputStreamHolder.fromStream(
                  new InputStream()
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
                  },
                  -1,
                  0
              )
          );
        }

        // Returns remaining timeout or throws exception if timeout already elapsed.
        private long checkQueryTimeout()
        {
          long timeLeft = timeoutAt - System.currentTimeMillis();
          if (timeLeft <= 0) {
            String msg = StringUtils.format("Query[%s] url[%s] timed out.", query.getId(), url);
            setupResponseReadFailure(msg, null);
            throw new QueryTimeoutException(msg);
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
            throw new ResourceLimitExceededException(msg);
          }
        }
      };

      long timeLeft = timeoutAt - System.currentTimeMillis();

      if (timeLeft <= 0) {
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query[%s] url[%s] timed out.", query.getId(), url));
      }

      future = httpClient.go(
          new Request(
              HttpMethod.POST,
              new URL(url)
          ).setContent(objectMapper.writeValueAsBytes(Queries.withTimeout(query, timeLeft)))
           .setHeader(
               HttpHeaders.Names.CONTENT_TYPE,
               isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON
           ),
          responseHandler,
          Duration.millis(timeLeft)
      );

      queryWatcher.registerQueryFuture(query, future);

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
                toolChest.decorateObjectMapper(objectMapper, query)
            );
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            CloseableUtils.closeAndWrapExceptions(iterFromMake);
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

  private void cancelQuery(Query<T> query, String cancelUrl)
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
            StatusResponseHolder response = responseFuture.get(30, TimeUnit.SECONDS);
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
          catch (TimeoutException e) {
            log.error(e, "Timed out cancelling query[%s]", query);
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
