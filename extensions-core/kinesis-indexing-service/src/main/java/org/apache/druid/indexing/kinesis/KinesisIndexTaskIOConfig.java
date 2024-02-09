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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Set;

public class KinesisIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
{
  public static final int DEFAULT_FETCH_DELAY_MILLIS = 0;

  /**
   * Together with {@link KinesisIndexTaskTuningConfig#MAX_RECORD_BUFFER_MEMORY}, don't take up more than 200MB
   * per task.
   */
  public static final int MAX_RECORD_FETCH_MEMORY = 100_000_000;

  /**
   * Together with {@link KinesisIndexTaskTuningConfig#RECORD_BUFFER_MEMORY_MAX_HEAP_FRACTION}, don't take up more
   * than 15% of the heap.
   */
  public static final double RECORD_FETCH_MEMORY_MAX_HEAP_FRACTION = 0.05;

  private final String endpoint;
  private final int fetchDelayMillis;

  private final String awsAssumedRoleArn;
  private final String awsExternalId;

  @JsonCreator
  public KinesisIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId,
      @JsonProperty("baseSequenceName") String baseSequenceName,
      // below three deprecated variables exist to be able to read old ioConfigs in metadata store
      @JsonProperty("startPartitions")
      @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<String, String> startPartitions,
      @JsonProperty("endPartitions")
      @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<String, String> endPartitions,
      @JsonProperty("exclusiveStartSequenceNumberPartitions")
      @Nullable
      @Deprecated Set<String> exclusiveStartSequenceNumberPartitions,
      // startSequenceNumbers and endSequenceNumbers must be set for new versions
      @JsonProperty("startSequenceNumbers") SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
      @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") String awsExternalId
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        getStartSequenceNumbers(startSequenceNumbers, startPartitions, exclusiveStartSequenceNumberPartitions),
        endSequenceNumbers == null ? endPartitions : endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );
    Preconditions.checkArgument(
        getEndSequenceNumbers().getPartitionSequenceNumberMap()
                               .values()
                               .stream()
                               .noneMatch(x -> x.equals(KinesisSequenceNumber.END_OF_SHARD_MARKER)),
        "End sequenceNumbers must not have the end of shard marker (EOS)"
    );

    this.endpoint = Preconditions.checkNotNull(endpoint, "endpoint");
    this.fetchDelayMillis = fetchDelayMillis != null ? fetchDelayMillis : DEFAULT_FETCH_DELAY_MILLIS;
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.awsExternalId = awsExternalId;
  }

  public KinesisIndexTaskIOConfig(
      int taskGroupId,
      String baseSequenceName,
      SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers,
      SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers,
      Boolean useTransaction,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      InputFormat inputFormat,
      String endpoint,
      Integer fetchDelayMillis,
      String awsAssumedRoleArn,
      String awsExternalId
  )
  {
    this(
        taskGroupId,
        baseSequenceName,
        null,
        null,
        null,
        startSequenceNumbers,
        endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat,
        endpoint,
        fetchDelayMillis,
        awsAssumedRoleArn,
        awsExternalId
    );
  }

  private static SeekableStreamStartSequenceNumbers<String, String> getStartSequenceNumbers(
      @Nullable SeekableStreamStartSequenceNumbers<String, String> newStartSequenceNumbers,
      @Nullable SeekableStreamEndSequenceNumbers<String, String> oldStartSequenceNumbers,
      @Nullable Set<String> exclusiveStartSequenceNumberPartitions
  )
  {
    if (newStartSequenceNumbers == null) {
      Preconditions.checkNotNull(
          oldStartSequenceNumbers,
          "Either startSequenceNumbers or startPartitions shoulnd't be null"
      );

      return new SeekableStreamStartSequenceNumbers<>(
          oldStartSequenceNumbers.getStream(),
          oldStartSequenceNumbers.getPartitionSequenceNumberMap(),
          exclusiveStartSequenceNumberPartitions
      );
    } else {
      return newStartSequenceNumbers;
    }
  }

  /**
   * This method is for compatibilty so that newer version of KinesisIndexTaskIOConfig can be read by
   * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
   * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
   *
   * A SeekableStreamEndSequenceNumbers (has no exclusivity info) is returned here because the Kinesis extension
   * previously stored exclusivity info separately in exclusiveStartSequenceNumberPartitions.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<String, String> getStartPartitions()
  {
    final SeekableStreamStartSequenceNumbers<String, String> startSequenceNumbers = getStartSequenceNumbers();
    return new SeekableStreamEndSequenceNumbers<>(
        startSequenceNumbers.getStream(),
        startSequenceNumbers.getPartitionSequenceNumberMap()
    );
  }

  /**
   * This method is for compatibilty so that newer version of KinesisIndexTaskIOConfig can be read by
   * old version of Druid.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<String, String> getEndPartitions()
  {
    return getEndSequenceNumbers();
  }

  @JsonProperty
  @Deprecated
  public Set<String> getExclusiveStartSequenceNumberPartitions()
  {
    return getStartSequenceNumbers().getExclusivePartitions();
  }

  @JsonProperty
  public String getEndpoint()
  {
    return endpoint;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getFetchDelayMillis()
  {
    return fetchDelayMillis;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAwsAssumedRoleArn()
  {
    return awsAssumedRoleArn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAwsExternalId()
  {
    return awsExternalId;
  }

  @Override
  public String toString()
  {
    return "KinesisIndexTaskIOConfig{" +
           "baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startPartitions=" + getStartSequenceNumbers() +
           ", endPartitions=" + getEndSequenceNumbers() +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           ", endpoint='" + endpoint + '\'' +
           ", fetchDelayMillis=" + fetchDelayMillis +
           ", awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", awsExternalId='" + awsExternalId + '\'' +
           '}';
  }
}
