/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.segment.indexing.IOConfig;

import java.util.Map;

public class KafkaIOConfig implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;

  private final String sequenceName;
  private final KafkaPartitions startPartitions;
  private final KafkaPartitions endPartitions;
  private final Map<String, String> consumerProperties;
  private final boolean useTransaction;

  @JsonCreator
  public KafkaIOConfig(
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("startPartitions") KafkaPartitions startPartitions,
      @JsonProperty("endPartitions") KafkaPartitions endPartitions,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("useTransaction") Boolean useTransaction
  )
  {
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
    this.startPartitions = Preconditions.checkNotNull(startPartitions, "startPartitions");
    this.endPartitions = Preconditions.checkNotNull(endPartitions, "endPartitions");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;

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
          "end offset must be >= start offset for partition[%d]",
          partition
      );
    }
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public KafkaPartitions getStartPartitions()
  {
    return startPartitions;
  }

  @JsonProperty
  public KafkaPartitions getEndPartitions()
  {
    return endPartitions;
  }

  @JsonProperty
  public Map<String, String> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public boolean isUseTransaction()
  {
    return useTransaction;
  }

  @Override
  public String toString()
  {
    return "KafkaIOConfig{" +
           "sequenceName='" + sequenceName + '\'' +
           ", startPartitions=" + startPartitions +
           ", endPartitions=" + endPartitions +
           ", consumerProperties=" + consumerProperties +
           ", useTransaction=" + useTransaction +
           '}';
  }
}
