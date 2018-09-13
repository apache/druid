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
import org.apache.druid.indexing.SeekableStream.SeekableStreamIOConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaIOConfig extends SeekableStreamIOConfig<Integer, Long>
{
  private static final boolean DEFAULT_SKIP_OFFSET_GAPS = false;

  @Nullable
  private final Map<String, String> consumerProperties;
  private final boolean skipOffsetGaps;

  @JsonCreator
  public KafkaIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") KafkaPartitions startPartitions,
      @JsonProperty("endPartitions") KafkaPartitions endPartitions,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        startPartitions,
        endPartitions,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.skipOffsetGaps = skipOffsetGaps != null ? skipOffsetGaps : DEFAULT_SKIP_OFFSET_GAPS;

    Preconditions.checkArgument(
        startPartitions.getTopic().equals(endPartitions.getTopic()),
        "start topic and end topic must match"
    );

    Preconditions.checkArgument(
        startPartitions.getPartitionOffsetMap().keySet().equals(endPartitions.getPartitionOffsetMap().keySet()),
        "start partition set and end partition set must match"
    );

    for (int partition : endPartitions.getPartitionOffsetMap().keySet()) {
      Preconditions.checkArgument(
          endPartitions.getPartitionOffsetMap().get(partition) >= startPartitions.getPartitionOffsetMap()
                                                                                 .get(partition),
          "end offset must be >= start offset for partition[%s]",
          partition
      );
    }
  }

  @Override
  @JsonProperty
  public KafkaPartitions getStartPartitions()
  {
    return (KafkaPartitions) super.getStartPartitions();
  }

  @Override
  @JsonProperty
  public KafkaPartitions getEndPartitions()
  {
    return (KafkaPartitions) super.getEndPartitions();
  }


  @Nullable
  @JsonProperty
  public Map<String, String> getConsumerProperties()
  {
    return consumerProperties;
  }


  @JsonProperty
  public boolean isSkipOffsetGaps()
  {
    return skipOffsetGaps;
  }

  @Override
  public String toString()
  {
    return "KafkaIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startPartitions=" + getStartPartitions() +
           ", endPartitions=" + getEndPartitions() +
           ", consumerProperties=" + consumerProperties +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           ", skipOffsetGaps=" + skipOffsetGaps +
           '}';
  }
}
