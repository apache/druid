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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Set;

public class KinesisIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, String>
{
  public static final int DEFAULT_RECORDS_PER_FETCH = 4000;
  public static final int DEFAULT_FETCH_DELAY_MILLIS = 0;

  private final String endpoint;
  private final Integer recordsPerFetch;
  private final Integer fetchDelayMillis;

  private final String awsAssumedRoleArn;
  private final String awsExternalId;
  private final boolean deaggregate;

  @JsonCreator
  public KinesisIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId,
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") SeekableStreamPartitions<String, String> startPartitions,
      @JsonProperty("endPartitions") SeekableStreamPartitions<String, String> endPartitions,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("recordsPerFetch") Integer recordsPerFetch,
      @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
      @JsonProperty("exclusiveStartSequenceNumberPartitions") Set<String> exclusiveStartSequenceNumberPartitions,
      @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") String awsExternalId,
      @JsonProperty("deaggregate") boolean deaggregate
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
        true,
        exclusiveStartSequenceNumberPartitions
    );
    Preconditions.checkArgument(endPartitions.getPartitionSequenceNumberMap()
                                             .values()
                                             .stream()
                                             .noneMatch(x -> x.equals(KinesisSequenceNumber.END_OF_SHARD_MARKER)));

    this.endpoint = Preconditions.checkNotNull(endpoint, "endpoint");
    this.recordsPerFetch = recordsPerFetch != null ? recordsPerFetch : DEFAULT_RECORDS_PER_FETCH;
    this.fetchDelayMillis = fetchDelayMillis != null ? fetchDelayMillis : DEFAULT_FETCH_DELAY_MILLIS;
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.awsExternalId = awsExternalId;
    this.deaggregate = deaggregate;
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
    return "KinesisIndexTaskIOConfig{" +
           "baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startPartitions=" + getStartPartitions() +
           ", endPartitions=" + getEndPartitions() +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           ", endpoint='" + endpoint + '\'' +
           ", recordsPerFetch=" + recordsPerFetch +
           ", fetchDelayMillis=" + fetchDelayMillis +
           ", exclusiveStartSequenceNumberPartitions=" + getExclusiveStartSequenceNumberPartitions() +
           ", awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", awsExternalId='" + awsExternalId + '\'' +
           ", deaggregate=" + deaggregate +
           '}';
  }
}
