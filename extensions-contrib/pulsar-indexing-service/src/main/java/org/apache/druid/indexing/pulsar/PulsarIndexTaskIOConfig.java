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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class PulsarIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
{
  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;

  @JsonCreator
  public PulsarIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      // startPartitions and endPartitions exist to be able to read old ioConfigs in metadata store
      @JsonProperty("startPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<String, String> startPartitions,
      @JsonProperty("endPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<String, String> endPartitions,
      // startSequenceNumbers and endSequenceNumbers must be set for new versions
      @JsonProperty("startSequenceNumbers")
      @Nullable SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers")
      @Nullable SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        startSequenceNumbers == null
        ? Preconditions.checkNotNull(startPartitions, "startPartitions").asStartPartitions(true)
        : startSequenceNumbers,
        endSequenceNumbers == null ? endPartitions : endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.pollTimeout = pollTimeout != null ? pollTimeout : PulsarSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

    final SeekableStreamEndSequenceNumbers<String, String> myEndSequenceNumbers = getEndSequenceNumbers();
    for (String partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          myEndSequenceNumbers.getPartitionSequenceNumberMap()
                       .get(partition)
                       .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(partition)) >= 0,
          "end offset must be >= start offset for partition[%s]",
          partition
      );
    }
  }

  public PulsarIndexTaskIOConfig(
      int taskGroupId,
      String baseSequenceName,
      SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
      SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers,
      Map<String, Object> consumerProperties,
      Long pollTimeout,
      Boolean useTransaction,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      InputFormat inputFormat
  )
  {
    this(
        taskGroupId,
        baseSequenceName,
        null,
        null,
        startSequenceNumbers,
        endSequenceNumbers,
        consumerProperties,
        pollTimeout,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );
  }

  /**
   * This method is for compatibilty so that newer version of PulsarIndexTaskIOConfig can be read by
   * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
   * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<String, String> getStartPartitions()
  {
    // Converting to start sequence numbers. This is allowed for Pulsar because the start offset is always inclusive.
    final SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers = getStartSequenceNumbers();
    return new SeekableStreamEndSequenceNumbers<>(
        startSequenceNumbers.getStream(),
        startSequenceNumbers.getPartitionSequenceNumberMap()
    );
  }

  /**
   * This method is for compatibilty so that newer version of PulsarIndexTaskIOConfig can be read by
   * old version of Druid.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<String, String> getEndPartitions()
  {
    return getEndSequenceNumbers();
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
    return "PulsarIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startSequenceNumbers=" + getStartSequenceNumbers() +
           ", endSequenceNumbers=" + getEndSequenceNumbers() +
           ", consumerProperties=" + consumerProperties +
           ", pollTimeout=" + pollTimeout +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }
}
