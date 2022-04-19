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

package org.apache.druid.indexing.pulsar;

import javax.validation.constraints.NotNull;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.util.MessageIdUtils;

public class PulsarSequenceNumber extends OrderedSequenceNumber<Long> {
  public static final Long LATEST_OFFSET = MessageIdUtils.getOffset(MessageId.latest);
  public static final Long EARLIEST_OFFSET = MessageIdUtils.getOffset(MessageId.earliest);

  protected PulsarSequenceNumber(Long sequenceNumber) {
    super(sequenceNumber, false);
  }
  public static PulsarSequenceNumber of(Long sequenceNumber)
  {
    return new PulsarSequenceNumber(sequenceNumber);
  }

  public static PulsarSequenceNumber of(MessageId id)
  {
    if (id.equals(MessageId.earliest)) {
      return new PulsarSequenceNumber(EARLIEST_OFFSET);
    }
    if (id.equals(MessageId.latest)) {
      return new PulsarSequenceNumber(LATEST_OFFSET);
    }
    return new PulsarSequenceNumber(MessageIdUtils.getOffset(id));
  }

  @Override
  public int compareTo(@NotNull OrderedSequenceNumber<Long> o) {
    return this.get().compareTo(o.get());
  }

  public MessageId getMessageId()
  {
    if (LATEST_OFFSET.equals(this.get())) {
      return MessageId.latest;
    }
    if (EARLIEST_OFFSET.equals(this.get())) {
      return MessageId.earliest;
    }
    return MessageIdUtils.getMessageId(this.get());
  }
}
