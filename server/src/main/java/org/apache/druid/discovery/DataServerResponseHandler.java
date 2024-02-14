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

package org.apache.druid.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.client.InputStreamHolder;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryTimeoutException;
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
 * Response handler for the {@link DataServerClient}. Handles the input stream from the data server and handles updating
 * the {@link ResponseContext} from the header. Does not apply backpressure or query timeout.
 */
public class DataServerResponseHandler implements HttpResponseHandler<InputStream, InputStream>
{
  private static final Logger log = new Logger(DataServerResponseHandler.class);
  private final Query<?> query;
  private final ResponseContext responseContext;
  private final AtomicLong totalByteCount = new AtomicLong(0);
  private final ObjectMapper objectMapper;
  private final AtomicReference<TrafficCop> trafficCopRef = new AtomicReference<>();
  private final long maxQueuedBytes;
  final boolean usingBackpressure;
  private final AtomicLong queuedByteCount = new AtomicLong(0);
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final BlockingQueue<InputStreamHolder> queue = new LinkedBlockingQueue<>();
  private final AtomicReference<String> fail = new AtomicReference<>();
  private final long failTime;

  public DataServerResponseHandler(Query<?> query, ResponseContext responseContext, ObjectMapper objectMapper)
  {
    this.query = query;
    this.responseContext = responseContext;
    this.objectMapper = objectMapper;
    QueryContext queryContext = query.context();
    maxQueuedBytes = queryContext.getMaxQueuedBytes(0);
    usingBackpressure = maxQueuedBytes > 0;
    long startTimeMillis = System.currentTimeMillis();
    if (queryContext.hasTimeout()) {
      failTime = startTimeMillis + queryContext.getTimeout();
    } else {
      failTime = 0;
    }
  }

  @Override
  public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    trafficCopRef.set(trafficCop);
    checkQueryTimeout();
    log.debug("Received response status[%s] for queryId[%s]", response.getStatus(), query.getId());

    final boolean continueReading;
    try {
      final String queryResponseHeaders = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
      if (queryResponseHeaders != null) {
        responseContext.merge(ResponseContext.deserialize(queryResponseHeaders, objectMapper));
      }

      continueReading = enqueue(response.getContent(), 0L);
    }
    catch (final IOException e) {
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
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
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

    boolean continueReading = true;
    if (bytes > 0) {
      try {
        continueReading = enqueue(channelBuffer, chunkNum);
      }
      catch (InterruptedException e) {
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
    log.debug("Finished reading response for queryId[%s]. Read total[%d]", query.getId(), totalByteCount.get());
    synchronized (done) {
      try {
        // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
        // after done is set to true, regardless of the rest of the stream's state.
        queue.put(InputStreamHolder.fromChannelBuffer(ChannelBuffers.EMPTY_BUFFER, Long.MAX_VALUE));
      }
      catch (InterruptedException e) {
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
  public void exceptionCaught(ClientResponse<InputStream> clientResponse, Throwable e)
  {
    String msg = StringUtils.format(
        "Query[%s] failed with exception msg [%s]",
        query.getId(),
        e.getMessage()
    );
    setupResponseReadFailure(msg, e);
  }

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
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query[%s] timed out.", query.getId()));
    }

    final long currentQueuedByteCount = queuedByteCount.addAndGet(-holder.getLength());
    if (usingBackpressure && currentQueuedByteCount < maxQueuedBytes) {
      Preconditions.checkNotNull(trafficCopRef.get(), "No TrafficCop, how can this be?").resume(holder.getChunkNum());
    }

    return holder.getStream();
  }


  // Returns remaining timeout or throws exception if timeout already elapsed.
  private long checkQueryTimeout()
  {
    long timeLeft = failTime - System.currentTimeMillis();
    if (timeLeft <= 0) {
      String msg = StringUtils.format("Query[%s] timed out.", query.getId());
      setupResponseReadFailure(msg, null);
      throw new QueryTimeoutException(msg);
    } else {
      return timeLeft;
    }
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
}
