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

package org.apache.druid.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.messages.client.MessageRelay;
import org.apache.druid.messages.server.MessageRelayResource;
import org.apache.druid.messages.server.Outbox;

import java.util.List;
import java.util.Objects;

/**
 * A batch of messages collected by {@link MessageRelay} from a remote {@link Outbox} through
 * {@link MessageRelayResource#httpGetMessagesFromOutbox}.
 */
public class MessageBatch<T>
{
  private final List<T> messages;
  private final long epoch;
  private final long startWatermark;

  @JsonCreator
  public MessageBatch(
      @JsonProperty("messages") final List<T> messages,
      @JsonProperty("epoch") final long epoch,
      @JsonProperty("watermark") final long startWatermark
  )
  {
    this.messages = messages;
    this.epoch = epoch;
    this.startWatermark = startWatermark;
  }

  /**
   * The messages.
   */
  @JsonProperty
  public List<T> getMessages()
  {
    return messages;
  }

  /**
   * Epoch, which is associated with a specific instance of {@link Outbox}.
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getEpoch()
  {
    return epoch;
  }

  /**
   * Watermark, an incrementing message ID that enables clients and servers to stay in sync, and enables
   * acknowledging of messages.
   */
  @JsonProperty("watermark")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getStartWatermark()
  {
    return startWatermark;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MessageBatch<?> that = (MessageBatch<?>) o;
    return epoch == that.epoch && startWatermark == that.startWatermark && Objects.equals(messages, that.messages);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(messages, epoch, startWatermark);
  }

  @Override
  public String toString()
  {
    return "MessageBatch{" +
           "messages=" + messages +
           ", epoch=" + epoch +
           ", startWatermark=" + startWatermark +
           '}';
  }
}
