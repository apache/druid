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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<Integer, Long>
{
  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;

  @JsonCreator
  public KafkaIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") SeekableStreamPartitions<Integer, Long> startPartitions,
      @JsonProperty("endPartitions") SeekableStreamPartitions<Integer, Long> endPartitions,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        startPartitions,
        endPartitions,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        null
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.pollTimeout = pollTimeout != null ? pollTimeout : KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

    for (int partition : endPartitions.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          endPartitions.getPartitionSequenceNumberMap()
                       .get(partition)
                       .compareTo(startPartitions.getPartitionSequenceNumberMap().get(partition)) >= 0,
          "end offset must be >= start offset for partition[%s]",
          partition
      );
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

  @Override
  public String toString()
  {
    return "KafkaIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startPartitions=" + getStartPartitions() +
           ", endPartitions=" + getEndPartitions() +
           ", consumerProperties=" + consumerProperties +
           ", pollTimeout=" + pollTimeout +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }
}
