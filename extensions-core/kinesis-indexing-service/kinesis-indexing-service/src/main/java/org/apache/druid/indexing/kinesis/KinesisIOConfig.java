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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;

import java.util.Set;

public class KinesisIOConfig implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_PAUSE_AFTER_READ = true;
  private static final int DEFAULT_RECORDS_PER_FETCH = 4000;
  private static final int DEFAULT_FETCH_DELAY_MILLIS = 0;

  private final String baseSequenceName;
  private final KinesisPartitions startPartitions;
  private final KinesisPartitions endPartitions;
  private final boolean useTransaction;
  private final boolean pauseAfterRead;
  private final Optional<DateTime> minimumMessageTime;
  private final Optional<DateTime> maximumMessageTime;
  private final String endpoint;
  private final Integer recordsPerFetch;
  private final Integer fetchDelayMillis;
  private final String awsAccessKeyId;
  private final String awsSecretAccessKey;
  private final Set<String> exclusiveStartSequenceNumberPartitions;
  private final String awsAssumedRoleArn;
  private final String awsExternalId;
  private final boolean deaggregate;

  @JsonCreator
  public KinesisIOConfig(
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") KinesisPartitions startPartitions,
      @JsonProperty("endPartitions") KinesisPartitions endPartitions,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("pauseAfterRead") Boolean pauseAfterRead,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("recordsPerFetch") Integer recordsPerFetch,
      @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
      @JsonProperty("awsAccessKeyId") String awsAccessKeyId,
      @JsonProperty("awsSecretAccessKey") String awsSecretAccessKey,
      @JsonProperty("exclusiveStartSequenceNumberPartitions") Set<String> exclusiveStartSequenceNumberPartitions,
      @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") String awsExternalId,
      @JsonProperty("deaggregate") boolean deaggregate
  )
  {
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.startPartitions = Preconditions.checkNotNull(startPartitions, "startPartitions");
    this.endPartitions = Preconditions.checkNotNull(endPartitions, "endPartitions");
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.pauseAfterRead = pauseAfterRead != null ? pauseAfterRead : DEFAULT_PAUSE_AFTER_READ;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
    this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);
    this.endpoint = Preconditions.checkNotNull(endpoint, "endpoint");
    this.recordsPerFetch = recordsPerFetch != null ? recordsPerFetch : DEFAULT_RECORDS_PER_FETCH;
    this.fetchDelayMillis = fetchDelayMillis != null ? fetchDelayMillis : DEFAULT_FETCH_DELAY_MILLIS;
    this.awsAccessKeyId = awsAccessKeyId;
    this.awsSecretAccessKey = awsSecretAccessKey;
    this.exclusiveStartSequenceNumberPartitions = exclusiveStartSequenceNumberPartitions;
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.awsExternalId = awsExternalId;
    this.deaggregate = deaggregate;

    Preconditions.checkArgument(
        startPartitions.getStream().equals(endPartitions.getStream()),
        "start stream and end stream must match"
    );

    Preconditions.checkArgument(
        startPartitions.getPartitionSequenceNumberMap()
                       .keySet()
                       .equals(endPartitions.getPartitionSequenceNumberMap().keySet()),
        "start partition set and end partition set must match"
    );
  }

  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @JsonProperty
  public KinesisPartitions getStartPartitions()
  {
    return startPartitions;
  }

  @JsonProperty
  public KinesisPartitions getEndPartitions()
  {
    return endPartitions;
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
  public Optional<DateTime> getMaximumMessageTime()
  {
    return maximumMessageTime;
  }

  @JsonProperty
  public String getEndpoint()
  {
    return endpoint;
  }

  @JsonProperty
  public int getRecordsPerFetch()
  {
    return recordsPerFetch;
  }

  @JsonProperty
  public int getFetchDelayMillis()
  {
    return fetchDelayMillis;
  }

  @JsonProperty
  public String getAwsAccessKeyId()
  {
    return awsAccessKeyId;
  }

  @JsonProperty
  public String getAwsSecretAccessKey()
  {
    return awsSecretAccessKey;
  }

  @JsonProperty
  public Set<String> getExclusiveStartSequenceNumberPartitions()
  {
    return exclusiveStartSequenceNumberPartitions;
  }

  @JsonProperty
  public String getAwsAssumedRoleArn()
  {
    return awsAssumedRoleArn;
  }

  @JsonProperty
  public String getAwsExternalId()
  {
    return awsExternalId;
  }

  @JsonProperty
  public boolean isDeaggregate()
  {
    return deaggregate;
  }

  @Override
  public String toString()
  {
    return "KinesisIOConfig{" +
           "baseSequenceName='" + baseSequenceName + '\'' +
           ", startPartitions=" + startPartitions +
           ", endPartitions=" + endPartitions +
           ", useTransaction=" + useTransaction +
           ", pauseAfterRead=" + pauseAfterRead +
           ", minimumMessageTime=" + minimumMessageTime +
           ", maximumMessageTime=" + maximumMessageTime +
           ", endpoint='" + endpoint + '\'' +
           ", recordsPerFetch=" + recordsPerFetch +
           ", fetchDelayMillis=" + fetchDelayMillis +
           ", awsAccessKeyId='" + awsAccessKeyId + '\'' +
           ", awsSecretAccessKey=" + "************************" +
           ", exclusiveStartSequenceNumberPartitions=" + exclusiveStartSequenceNumberPartitions +
           ", awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", awsExternalId='" + awsExternalId + '\'' +
           ", deaggregate=" + deaggregate +
           '}';
  }
}
