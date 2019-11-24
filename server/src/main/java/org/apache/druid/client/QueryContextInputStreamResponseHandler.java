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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResource;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An HTTP response handler which uses sequence input streams to create a final InputStream.
 *
 * This implementation takes query context into consideration, e.g. timeout, maxQueuedBytes, backpressure.
 * It uses a blocking queue to feed a SequenceInputStream that is terminated whenever the handler's
 * {@link #done} is called or a throwable is detected.
 *
 * The resulting InputStream will attempt to terminate normally, but on exception in HttpResponseHandler
 * may end with an IOException upon {@link InputStream#read()}
 *
 * {@link org.apache.druid.java.util.http.client.response.SequenceInputStreamResponseHandler} also uses
 * sequence input streams to create final InputStream, but it does not have timeout, backpressure,
 * or any query context. 
 */
public class QueryContextInputStreamResponseHandler implements HttpResponseHandler<InputStream, InputStream>
{
  private static final Logger log = new Logger(QueryContextInputStreamResponseHandler.class);

  private final long requestStartTimeNs = System.nanoTime();
  private final AtomicLong totalByteCount = new AtomicLong(0);
  private final AtomicLong queuedByteCount = new AtomicLong(0);
  private final AtomicLong channelSuspendedTime = new AtomicLong(0);
  private final BlockingQueue<InputStreamHolder> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final AtomicReference<String> fail = new AtomicReference<>();
  private final AtomicReference<TrafficCop> trafficCopRef = new AtomicReference<>();

  private final Query<?> query;
  private final ResponseContext context;
  private final String url;
  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper objectMapper;
  private final long timeoutAt;
  private final long maxScatterGatherBytes;
  private final AtomicLong totalBytesGathered;
  private final long maxQueuedBytes;
  private final boolean usingBackpressure;

  public QueryContextInputStreamResponseHandler(
      Query<?> query,
      ResponseContext context,
      QueryToolChestWarehouse warehouse,
      String url,
      long timeoutAt,
      ObjectMapper objectMapper
  )
  {
    this.query = query;
    this.context = context;
    this.warehouse = warehouse;
    this.url = url;
    this.objectMapper = objectMapper;
    this.timeoutAt = timeoutAt;
    this.maxScatterGatherBytes = QueryContexts.getMaxScatterGatherBytes(query);
    this.totalBytesGathered = (AtomicLong) context.get(ResponseContext.Key.QUERY_TOTAL_BYTES_GATHERED);
    this.maxQueuedBytes = QueryContexts.getMaxQueuedBytes(query, 0);
    this.usingBackpressure = maxQueuedBytes > 0;
  }

  @Override
  public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    trafficCopRef.set(trafficCop);
    checkQueryTimeout();
    checkTotalBytesLimit(response.getContent().readableBytes());

    log.debug("Initial response from url[%s] for queryId[%s]", url, query.getId());

    final boolean continueReading;
    try {
      final String responseContext = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
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
      throw new RE("Query[%s] url[%s] timed out.", query.getId(), url);
    }

    final long currentQueuedByteCount = queuedByteCount.addAndGet(-holder.getLength());
    if (usingBackpressure && currentQueuedByteCount < maxQueuedBytes) {
      long backPressureTime = Preconditions.checkNotNull(trafficCopRef.get(), "No TrafficCop, how can this be?")
                                            .resume(holder.getChunkNum());
      channelSuspendedTime.addAndGet(backPressureTime);
    }

    return holder.getStream();
  }
}
