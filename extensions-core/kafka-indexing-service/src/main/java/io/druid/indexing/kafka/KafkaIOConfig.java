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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;

import java.util.Map;

public class KafkaIOConfig implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_PAUSE_AFTER_READ = false;
  private static final boolean DEFAULT_SKIP_OFFSET_GAPS = false;

  private final String baseSequenceName;
  private final KafkaPartitions startPartitions;
  private final KafkaPartitions endPartitions;
  private final Map<String, String> consumerProperties;
  private final boolean useTransaction;
  private final boolean pauseAfterRead;
  private final Optional<DateTime> minimumMessageTime;
  private final boolean skipOffsetGaps;

  @JsonCreator
  public KafkaIOConfig(
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") KafkaPartitions startPartitions,
      @JsonProperty("endPartitions") KafkaPartitions endPartitions,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("pauseAfterRead") Boolean pauseAfterRead,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps
  )
  {
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.startPartitions = Preconditions.checkNotNull(startPartitions, "startPartitions");
    this.endPartitions = Preconditions.checkNotNull(endPartitions, "endPartitions");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.pauseAfterRead = pauseAfterRead != null ? pauseAfterRead : DEFAULT_PAUSE_AFTER_READ;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
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

  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
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

  @JsonProperty
  public boolean isPauseAfterRead()
  {
    return pauseAfterRead;
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime()
  {
    return minimumMessageTime;
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
           "baseSequenceName='" + baseSequenceName + '\'' +
           ", startPartitions=" + startPartitions +
           ", endPartitions=" + endPartitions +
           ", consumerProperties=" + consumerProperties +
           ", useTransaction=" + useTransaction +
           ", pauseAfterRead=" + pauseAfterRead +
           ", minimumMessageTime=" + minimumMessageTime +
           ", skipOffsetGaps=" + skipOffsetGaps +
           '}';
  }
}
