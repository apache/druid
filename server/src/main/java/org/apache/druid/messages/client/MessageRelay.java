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

package org.apache.druid.messages.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.server.MessageRelayResource;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.server.DruidNode;

import java.io.Closeable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Relays run on clients, and receive messages from a server.
 * Uses {@link MessageRelayClient} to communicate with the {@link MessageRelayResource} on a server.
 * that flows upstream
 */
public class MessageRelay<MessageType> implements Closeable
{
  private static final Logger log = new Logger(MessageRelay.class);

  /**
   * Value to provide for epoch on the initial call to {@link MessageRelayClient#getMessages(String, long, long)}.
   */
  public static final long INIT = -1;

  private final String selfHost;
  private final DruidNode serverNode;
  private final MessageRelayClient<MessageType> client;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Collector collector;

  public MessageRelay(
      final String selfHost,
      final DruidNode serverNode,
      final MessageRelayClient<MessageType> client,
      final MessageListener<MessageType> listener
  )
  {
    this.selfHost = selfHost;
    this.serverNode = serverNode;
    this.client = client;
    this.collector = new Collector(listener);
  }

  /**
   * Start the {@link Collector}.
   */
  public void start()
  {
    collector.start();
  }

  /**
   * Stop the {@link Collector}.
   */
  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      collector.stop();
    }
  }

  /**
   * Retrieves messages that are being sent to this client and hands them to {@link #listener}.
   */
  private class Collector
  {
    private final MessageListener<MessageType> listener;
    private final AtomicLong epoch = new AtomicLong(INIT);
    private final AtomicLong watermark = new AtomicLong(INIT);
    private final AtomicReference<ListenableFuture<?>> currentCall = new AtomicReference<>();

    public Collector(final MessageListener<MessageType> listener)
    {
      this.listener = listener;
    }

    private void start()
    {
      if (!watermark.compareAndSet(INIT, 0)) {
        throw new ISE("Already started");
      }

      listener.serverAdded(serverNode);
      issueNextGetMessagesCall();
    }

    private void issueNextGetMessagesCall()
    {
      if (closed.get()) {
        return;
      }

      final long theEpoch = epoch.get();
      final long theWatermark = watermark.get();

      log.debug(
          "Getting messages from server[%s] for client[%s] (current state: epoch[%s] watermark[%s]).",
          serverNode.getHostAndPortToUse(),
          selfHost,
          theEpoch,
          theWatermark
      );

      final ListenableFuture<MessageBatch<MessageType>> future = client.getMessages(selfHost, theEpoch, theWatermark);

      if (!currentCall.compareAndSet(null, future)) {
        log.error(
            "Fatal error: too many outgoing calls to server[%s] for client[%s] "
            + "(current state: epoch[%s] watermark[%s]). Closing collector.",
            serverNode.getHostAndPortToUse(),
            selfHost,
            theEpoch,
            theWatermark
        );

        close();
        return;
      }

      Futures.addCallback(
          future,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(final MessageBatch<MessageType> result)
            {
              log.debug("Received message batch: %s", result);
              currentCall.compareAndSet(future, null);
              final long endWatermark = result.getStartWatermark() + result.getMessages().size();
              if (theEpoch == INIT) {
                epoch.set(result.getEpoch());
                watermark.set(endWatermark);
              } else if (epoch.get() != result.getEpoch()
                         || !watermark.compareAndSet(result.getStartWatermark(), endWatermark)) {
                // We don't expect to see this unless there is somehow another collector running with the same
                // clientHost. If the unexpected happens, log it and close the collector. It will stay, doing
                // nothing, in the MessageCollectors map until it is removed by the discovery listener.
                log.error(
                    "Incorrect epoch + watermark from server[%s] for client[%s] "
                    + "(expected[%s:%s] but got[%s:%s]). "
                    + "Closing collector.",
                    serverNode.getHostAndPortToUse(),
                    selfHost,
                    theEpoch,
                    theWatermark,
                    result.getEpoch(),
                    result.getStartWatermark()
                );

                close();
                return;
              }

              for (final MessageType message : result.getMessages()) {
                try {
                  listener.messageReceived(message);
                }
                catch (Throwable e) {
                  log.warn(
                      e,
                      "Failed to handle message[%s] from server[%s] for client[%s].",
                      message,
                      selfHost,
                      serverNode.getHostAndPortToUse()
                  );
                }
              }

              issueNextGetMessagesCall();
            }

            @Override
            public void onFailure(final Throwable e)
            {
              currentCall.compareAndSet(future, null);
              if (!(e instanceof CancellationException) && !(e instanceof ServiceClosedException)) {
                // We don't expect to see any other errors, since we use an unlimited retry policy for clients. If the
                // unexpected happens, log it and close the collector. It will stay, doing nothing, in the
                // MessageCollectors map until it is removed by the discovery listener.
                log.error(
                    e,
                    "Fatal error contacting server[%s] for client[%s] "
                    + "(current state: epoch[%s] watermark[%s]). "
                    + "Closing collector.",
                    serverNode.getHostAndPortToUse(),
                    selfHost,
                    theEpoch,
                    theWatermark
                );
              }

              close();
            }
          },
          Execs.directExecutor()
      );
    }

    public void stop()
    {
      final ListenableFuture<?> future = currentCall.getAndSet(null);
      if (future != null) {
        future.cancel(true);
      }

      try {
        listener.serverRemoved(serverNode);
      }
      catch (Throwable e) {
        log.warn(e, "Failed to close server[%s]", serverNode.getHostAndPortToUse());
      }
    }
  }
}
