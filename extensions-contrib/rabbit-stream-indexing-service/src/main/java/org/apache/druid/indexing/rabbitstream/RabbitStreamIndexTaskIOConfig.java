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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.rabbitstream.supervisor.RabbitStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class RabbitStreamIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, Long>
{
  private final long pollTimeout;
  private final String uri;
  private final Map<String, Object> consumerProperties;

  @JsonCreator
  public RabbitStreamIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null
                                                                  // for
                                                                  // backward
                                                                  // compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startSequenceNumbers") SeekableStreamStartSequenceNumbers<String, Long> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") SeekableStreamEndSequenceNumbers<String, Long> endSequenceNumbers,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("uri") String uri)
  {
    super(
        taskGroupId,
        baseSequenceName,
        startSequenceNumbers,
        endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat);

    this.pollTimeout = pollTimeout != null ? pollTimeout : RabbitStreamSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
    this.uri = uri;

    this.consumerProperties = consumerProperties;

    final SeekableStreamEndSequenceNumbers<String, Long> myEndSequenceNumbers = getEndSequenceNumbers();
    for (String partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          myEndSequenceNumbers.getPartitionSequenceNumberMap()
              .get(partition)
              .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(partition)) >= 0,
          "end offset must be >= start offset for partition[%s]",
          partition);
    }
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public String getUri()
  {
    return this.uri;
  }

  @Override
  public String toString()
  {
    return "RabbitStreamIndexTaskIOConfig{" +
        "taskGroupId=" + getTaskGroupId() +
        ", baseSequenceName='" + getBaseSequenceName() + '\'' +
        ", startSequenceNumbers=" + getStartSequenceNumbers() +
        ", endSequenceNumbers=" + getEndSequenceNumbers() +
        ", consumerProperties=" + consumerProperties +
        ", pollTimeout=" + pollTimeout +
        ", useTransaction=" + isUseTransaction() +
        ", minimumMessageTime=" + getMinimumMessageTime() +
        ", maximumMessageTime=" + getMaximumMessageTime() +
        ", uri=" + getUri() +
        '}';
  }
}
