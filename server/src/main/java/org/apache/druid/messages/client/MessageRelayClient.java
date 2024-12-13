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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.server.MessageRelayResource;

/**
 * Client for {@link MessageRelayResource}.
 */
public interface MessageRelayClient<MessageType>
{
  /**
   * Get the next batch of messages from an outbox.
   *
   * @param clientHost     which outbox to retrieve messages from. Each clientHost has its own outbox.
   * @param epoch          outbox epoch, or {@link MessageRelay#INIT} if this is the first call from the collector.
   * @param startWatermark outbox message watermark to retrieve from.
   *
   * @return future that resolves to the next batch of messages
   *
   * @see MessageRelayResource#httpGetMessagesFromOutbox http endpoint this method calls
   */
  ListenableFuture<MessageBatch<MessageType>> getMessages(String clientHost, long epoch, long startWatermark);
}
