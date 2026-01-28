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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.client.MessageRelay;

/**
 * An outbox for messages sent from servers to clients. Messages are retrieved in the order they are sent.
 *
 * @see org.apache.druid.messages package-level javadoc for description of the message relay system
 */
public interface Outbox<MessageType>
{
  /**
   * Send a message to a client, through an outbox.
   *
   * @param clientHost which outbox to send messages through. Each clientHost has its own outbox.
   * @param message    message to send
   *
   * @return future that resolves successfully when the client has acknowledged the message
   */
  ListenableFuture<?> sendMessage(String clientHost, MessageType message);

  /**
   * Get the next batch of messages for an client, from an outbox. Messages are retrieved in the order they were sent.
   *
   * The provided epoch must either be {@link MessageRelay#INIT}, or must match the epoch of the outbox as indicated by
   * {@link MessageBatch#getEpoch()} returned by previous calls to the same outbox. If the provided epoch does not
   * match, an empty batch is returned with the correct epoch indicated in {@link MessageBatch#getEpoch()}.
   *
   * The provided watermark must be greater than, or equal to, the previous watermark supplied to the same outbox.
   * Any messages lower than the watermark are acknowledged and removed from the outbox.
   *
   * @param clientHost     which outbox to retrieve messages from. Each clientHost has its own outbox.
   * @param epoch          outbox epoch, or {@link MessageRelay#INIT} if this is the first call from the collector.
   * @param startWatermark outbox message watermark to retrieve from.
   *
   * @return future that resolves to the next batch of messages
   */
  ListenableFuture<MessageBatch<MessageType>> getMessages(String clientHost, long epoch, long startWatermark);

  /**
   * Reset the outbox for a particular client. This removes all messages, cancels all outstanding futures, and
   * resets the epoch.
   *
   * @param clientHost the client host:port
   */
  void resetOutbox(String clientHost);
}
