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

package org.apache.druid.messages.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.common.guava.FutureBox;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.client.MessageRelay;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Production implementation of {@link Outbox}. Each outbox is represented by an {@link OutboxQueue}.
 */
public class OutboxImpl<MessageType> implements Outbox<MessageType>
{
  private static final int MAX_BATCH_SIZE = 8;

  // clientHost -> outgoing message queue
  private final ConcurrentHashMap<String, OutboxQueue<MessageType>> queues;
  private volatile boolean stopped;

  public OutboxImpl()
  {
    this.queues = new ConcurrentHashMap<>();
  }

  @LifecycleStop
  public void stop()
  {
    stopped = true;

    final Iterator<OutboxQueue<MessageType>> it = queues.values().iterator();
    while (it.hasNext()) {
      it.next().stop();
      it.remove();
    }
  }

  @Override
  public ListenableFuture<?> sendMessage(String clientHost, MessageType message)
  {
    if (stopped) {
      return Futures.immediateCancelledFuture();
    }

    return queues.computeIfAbsent(clientHost, id -> new OutboxQueue<>())
                 .sendMessage(message);
  }

  @Override
  public ListenableFuture<MessageBatch<MessageType>> getMessages(String clientHost, long epoch, long startWatermark)
  {
    if (stopped) {
      return Futures.immediateCancelledFuture();
    }

    final OutboxQueue<MessageType> queue = queues.computeIfAbsent(clientHost, id -> new OutboxQueue<>());
    if (epoch != queue.epoch && epoch != MessageRelay.INIT) {
      return Futures.immediateFuture(new MessageBatch<>(Collections.emptyList(), queue.epoch, 0));
    }

    return queue.getMessages(startWatermark);
  }

  @Override
  public void resetOutbox(final String clientHost)
  {
    final OutboxQueue<MessageType> queue = queues.remove(clientHost);
    if (queue != null) {
      queue.stop();
    }
  }

  @VisibleForTesting
  long getOutboxEpoch(final String clientHost)
  {
    final OutboxQueue<MessageType> queue = queues.get(clientHost);
    return queue != null ? queue.epoch : MessageRelay.INIT;
  }

  /**
   * Outgoing queue for a specific client.
   */
  public static class OutboxQueue<T>
  {
    /**
     * Epoch, set when the outbox is created. Attached to returned batches through {@link MessageBatch#getEpoch()}.
     */
    private final long epoch;

    /**
     * Currently-outstanding futures.
     */
    private final FutureBox pendingFutures = new FutureBox();

    @GuardedBy("this")
    private long startWatermark = 0;

    @GuardedBy("this")
    private final Deque<Pair<SettableFuture<?>, T>> queue = new ArrayDeque<>();

    @GuardedBy("this")
    private SettableFuture<?> messageAvailableFuture = SettableFuture.create();

    public OutboxQueue()
    {
      // Random positive number, to differentiate this outbox from a previous version that may have lived
      // on the same host. (When the upstream relay connects, it needs to know if this is the "same" outbox
      // it was previously listening to.)
      this.epoch = ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;
    }

    ListenableFuture<?> sendMessage(final T message)
    {
      final SettableFuture<?> future = SettableFuture.create();

      synchronized (this) {
        queue.add(Pair.of(future, message));
        if (!messageAvailableFuture.isDone()) {
          messageAvailableFuture.set(null);
        }
      }

      return pendingFutures.register(future);
    }

    ListenableFuture<MessageBatch<T>> getMessages(final long newStartWatermark)
    {
      synchronized (this) {
        // Ack and drain all messages up to startWatermark.
        while (!queue.isEmpty() && startWatermark < newStartWatermark) {
          final Pair<SettableFuture<?>, T> message = queue.poll();
          startWatermark++;
          message.lhs.set(null);
        }

        if (queue.isEmpty()) {
          // Send next batch when a message is available.
          if (messageAvailableFuture.isDone()) {
            messageAvailableFuture = SettableFuture.create();
          }

          return pendingFutures.register(
              FutureUtils.transform(
                  Futures.nonCancellationPropagating(messageAvailableFuture),
                  ignored -> {
                    synchronized (this) {
                      return nextBatch();
                    }
                  }
              )
          );
        } else {
          return pendingFutures.register(Futures.immediateFuture(nextBatch()));
        }
      }
    }

    void stop()
    {
      pendingFutures.close();
    }

    @GuardedBy("this")
    private MessageBatch<T> nextBatch()
    {
      final List<T> batch = new ArrayList<>();
      final Iterator<Pair<SettableFuture<?>, T>> it = queue.iterator();

      while (it.hasNext() && batch.size() < MAX_BATCH_SIZE) {
        batch.add(it.next().rhs);
      }

      return new MessageBatch<>(batch, epoch, startWatermark);
    }
  }
}
