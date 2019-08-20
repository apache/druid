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

package org.apache.druid.java.util.http.client.response;

import com.google.common.io.ByteSource;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.logger.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A HTTP response handler which uses sequence input streams to create a final InputStream.
 * Any particular instance is encouraged to overwrite a method and call the super if they need extra handling of the
 * method parameters.
 *
 * This implementation uses a blocking queue to feed a SequenceInputStream that is terminated whenever the handler's Done
 * method is called or a throwable is detected.
 *
 * The resulting InputStream will attempt to terminate normally, but on exception in HttpResponseHandler
 * may end with an IOException upon read()
 */
public class SequenceInputStreamResponseHandler implements HttpResponseHandler<InputStream, InputStream>
{
  private static final Logger log = new Logger(SequenceInputStreamResponseHandler.class);
  private final AtomicLong byteCount = new AtomicLong(0);
  private final BlockingQueue<InputStream> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean done = new AtomicBoolean(false);

  @Override
  public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
  {
    ChannelBufferInputStream channelStream = null;
    try {
      channelStream = new ChannelBufferInputStream(response.getContent());
      queue.put(channelStream);
    }
    catch (InterruptedException e) {
      log.error(e, "Queue appending interrupted");
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    finally {
      CloseQuietly.close(channelStream);
    }
    byteCount.addAndGet(response.getContent().readableBytes());
    return ClientResponse.finished(
        new SequenceInputStream(
            new Enumeration<InputStream>()
            {
              @Override
              public boolean hasMoreElements()
              {
                // Done is always true until the last stream has be put in the queue.
                // Then the stream should be spouting good InputStreams.
                synchronized (done) {
                  return !done.get() || !queue.isEmpty();
                }
              }

              @Override
              public InputStream nextElement()
              {
                try {
                  return queue.take();
                }
                catch (InterruptedException e) {
                  log.warn(e, "Thread interrupted while taking from queue");
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
            }
        )
    );
  }

  @Override
  public ClientResponse<InputStream> handleChunk(
      ClientResponse<InputStream> clientResponse,
      HttpChunk chunk,
      long chunkNum
  )
  {
    final ChannelBuffer channelBuffer = chunk.getContent();
    final int bytes = channelBuffer.readableBytes();
    if (bytes > 0) {
      ChannelBufferInputStream channelStream = null;
      try {
        channelStream = new ChannelBufferInputStream(channelBuffer);
        queue.put(channelStream);
        // Queue.size() can be expensive in some implementations, but LinkedBlockingQueue.size is just an AtomicLong
        log.debug("Added stream. Queue length %d", queue.size());
      }
      catch (InterruptedException e) {
        log.warn(e, "Thread interrupted while adding to queue");
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      finally {
        CloseQuietly.close(channelStream);
      }
      byteCount.addAndGet(bytes);
    } else {
      log.debug("Skipping zero length chunk");
    }
    return clientResponse;
  }

  @Override
  public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
  {
    synchronized (done) {
      try {
        // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
        // after done is set to true, regardless of the rest of the stream's state.
        queue.put(ByteSource.empty().openStream());
        log.debug("Added terminal empty stream");
      }
      catch (InterruptedException e) {
        log.warn(e, "Thread interrupted while adding to queue");
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (IOException e) {
        // This should never happen
        log.wtf(e, "The empty stream threw an IOException");
        throw new RuntimeException(e);
      }
      finally {
        log.debug("Done after adding %d bytes of streams", byteCount.get());
        done.set(true);
      }
    }
    return ClientResponse.finished(clientResponse.getObj());
  }

  @Override
  public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
  {
    // Don't wait for lock in case the lock had something to do with the error
    synchronized (done) {
      done.set(true);
      // Make a best effort to put a zero length buffer into the queue in case something is waiting on the take()
      // If nothing is waiting on take(), this will be closed out anyways.
      final boolean accepted = queue.offer(
          new InputStream()
          {
            @Override
            public int read() throws IOException
            {
              throw new IOException(e);
            }
          }
      );
      if (!accepted) {
        log.warn("Unable to place final IOException offer in queue");
      } else {
        log.debug("Placed IOException in queue");
      }
      log.debug(e, "Exception with queue length of %d and %d bytes available", queue.size(), byteCount.get());
    }
  }

  public final long getByteCount()
  {
    return byteCount.get();
  }
}
