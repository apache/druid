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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.segment.indexing.IOConfig;
import org.apache.druid.storage.StorageConnectorProvider;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * IO configuration for {@link ShareGroupIndexTask}.
 *
 * Unlike {@link KafkaIndexTaskIOConfig}, this config does not carry start/end
 * offsets because the Kafka broker manages offset tracking for share groups.
 * A durable inbox stores acquired records before they are acknowledged and processed.
 */
public class ShareGroupIndexTaskIOConfig implements IOConfig
{
  private static final long DEFAULT_POLL_TIMEOUT_MILLIS = KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
  private static final int DEFAULT_RECEIPT_PAGE_SIZE = 4_096;
  private static final int DEFAULT_MAX_STAGING_RECORDS = 10_000;
  private static final long DEFAULT_MAX_STAGING_BYTES = 256L << 20;
  private static final int DEFAULT_MAX_CONCURRENT_UPLOADS = 2;
  private static final double DEFAULT_RENEWAL_FRACTION = 0.5;
  private static final int DEFAULT_MAX_PROCESSING_MANIFESTS = 16;
  private static final int DEFAULT_MAX_PROCESSING_RECORDS = 100_000;
  private static final long DEFAULT_MAX_PROCESSING_BYTES = 256L << 20;
  private static final long DEFAULT_CLAIM_DURATION_MILLIS = 300_000L;
  private static final long DEFAULT_CLAIM_RENEWAL_PERIOD_MILLIS = 60_000L;
  private static final long DEFAULT_INBOX_POLL_PERIOD_MILLIS = 1_000L;

  private final String topic;
  private final String groupId;
  private final Map<String, Object> consumerProperties;
  private final InputFormat inputFormat;
  private final long pollTimeout;
  private final String inboxId;
  private final StorageConnectorProvider inboxStorage;
  private final int receiptPageSize;
  private final int maxStagingRecords;
  private final long maxStagingBytes;
  private final int maxConcurrentUploads;
  private final double renewalFraction;
  private final int maxProcessingManifests;
  private final int maxProcessingRecords;
  private final long maxProcessingBytes;
  private final long claimDurationMillis;
  private final long claimRenewalPeriodMillis;
  private final long inboxPollPeriodMillis;

  public ShareGroupIndexTaskIOConfig(
      String topic,
      String groupId,
      Map<String, Object> consumerProperties,
      @Nullable InputFormat inputFormat,
      @Nullable Long pollTimeout,
      String inboxId,
      StorageConnectorProvider inboxStorage
  )
  {
    this(
        topic,
        groupId,
        consumerProperties,
        inputFormat,
        pollTimeout,
        inboxId,
        inboxStorage,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @JsonCreator
  public ShareGroupIndexTaskIOConfig(
      @JsonProperty("topic") String topic,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("pollTimeout") @Nullable Long pollTimeout,
      @JsonProperty("inboxId") String inboxId,
      @JsonProperty("inboxStorage") StorageConnectorProvider inboxStorage,
      @JsonProperty("receiptPageSize") @Nullable Integer receiptPageSize,
      @JsonProperty("maxStagingRecords") @Nullable Integer maxStagingRecords,
      @JsonProperty("maxStagingBytes") @Nullable Long maxStagingBytes,
      @JsonProperty("maxConcurrentUploads") @Nullable Integer maxConcurrentUploads,
      @JsonProperty("renewalFraction") @Nullable Double renewalFraction,
      @JsonProperty("maxProcessingManifests") @Nullable Integer maxProcessingManifests,
      @JsonProperty("maxProcessingRecords") @Nullable Integer maxProcessingRecords,
      @JsonProperty("maxProcessingBytes") @Nullable Long maxProcessingBytes,
      @JsonProperty("claimDurationMillis") @Nullable Long claimDurationMillis,
      @JsonProperty("claimRenewalPeriodMillis") @Nullable Long claimRenewalPeriodMillis,
      @JsonProperty("inboxPollPeriodMillis") @Nullable Long inboxPollPeriodMillis
  )
  {
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.inputFormat = inputFormat;
    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
    this.inboxId = Preconditions.checkNotNull(inboxId, "inboxId");
    this.inboxStorage = Preconditions.checkNotNull(inboxStorage, "inboxStorage");
    this.receiptPageSize = receiptPageSize != null ? receiptPageSize : DEFAULT_RECEIPT_PAGE_SIZE;
    this.maxStagingRecords = maxStagingRecords != null ? maxStagingRecords : DEFAULT_MAX_STAGING_RECORDS;
    this.maxStagingBytes = maxStagingBytes != null ? maxStagingBytes : DEFAULT_MAX_STAGING_BYTES;
    this.maxConcurrentUploads = maxConcurrentUploads != null
                                ? maxConcurrentUploads
                                : DEFAULT_MAX_CONCURRENT_UPLOADS;
    this.renewalFraction = renewalFraction != null ? renewalFraction : DEFAULT_RENEWAL_FRACTION;
    this.maxProcessingManifests = maxProcessingManifests != null
                                  ? maxProcessingManifests
                                  : DEFAULT_MAX_PROCESSING_MANIFESTS;
    this.maxProcessingRecords = maxProcessingRecords != null
                                ? maxProcessingRecords
                                : DEFAULT_MAX_PROCESSING_RECORDS;
    this.maxProcessingBytes = maxProcessingBytes != null ? maxProcessingBytes : DEFAULT_MAX_PROCESSING_BYTES;
    this.claimDurationMillis = claimDurationMillis != null ? claimDurationMillis : DEFAULT_CLAIM_DURATION_MILLIS;
    this.claimRenewalPeriodMillis = claimRenewalPeriodMillis != null
                                    ? claimRenewalPeriodMillis
                                    : DEFAULT_CLAIM_RENEWAL_PERIOD_MILLIS;
    this.inboxPollPeriodMillis = inboxPollPeriodMillis != null
                                 ? inboxPollPeriodMillis
                                 : DEFAULT_INBOX_POLL_PERIOD_MILLIS;

    Preconditions.checkArgument(this.pollTimeout >= 0, "pollTimeout must not be negative");
    Preconditions.checkArgument(!this.inboxId.isEmpty(), "inboxId must not be empty");
    Preconditions.checkArgument(this.receiptPageSize > 0, "receiptPageSize must be positive");
    Preconditions.checkArgument(this.maxStagingRecords > 0, "maxStagingRecords must be positive");
    Preconditions.checkArgument(this.maxStagingBytes > 0, "maxStagingBytes must be positive");
    Preconditions.checkArgument(this.maxConcurrentUploads > 0, "maxConcurrentUploads must be positive");
    Preconditions.checkArgument(
        this.renewalFraction > 0 && this.renewalFraction <= 1,
        "renewalFraction must be in (0, 1]"
    );
    Preconditions.checkArgument(this.maxProcessingManifests > 0, "maxProcessingManifests must be positive");
    Preconditions.checkArgument(this.maxProcessingRecords > 0, "maxProcessingRecords must be positive");
    Preconditions.checkArgument(this.maxProcessingBytes > 0, "maxProcessingBytes must be positive");
    Preconditions.checkArgument(this.claimDurationMillis > 0, "claimDurationMillis must be positive");
    Preconditions.checkArgument(this.claimRenewalPeriodMillis > 0, "claimRenewalPeriodMillis must be positive");
    Preconditions.checkArgument(
        this.claimRenewalPeriodMillis < this.claimDurationMillis,
        "claimRenewalPeriodMillis must be less than claimDurationMillis"
    );
    Preconditions.checkArgument(this.inboxPollPeriodMillis > 0, "inboxPollPeriodMillis must be positive");
  }

  @JsonProperty
  public String getTopic()
  {
    return topic;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public InputFormat getInputFormat()
  {
    return inputFormat;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public String getInboxId()
  {
    return inboxId;
  }

  @JsonProperty
  public StorageConnectorProvider getInboxStorage()
  {
    return inboxStorage;
  }

  @JsonProperty
  public int getReceiptPageSize()
  {
    return receiptPageSize;
  }

  @JsonProperty
  public int getMaxStagingRecords()
  {
    return maxStagingRecords;
  }

  @JsonProperty
  public long getMaxStagingBytes()
  {
    return maxStagingBytes;
  }

  @JsonProperty
  public int getMaxConcurrentUploads()
  {
    return maxConcurrentUploads;
  }

  @JsonProperty
  public double getRenewalFraction()
  {
    return renewalFraction;
  }

  @JsonProperty
  public int getMaxProcessingManifests()
  {
    return maxProcessingManifests;
  }

  @JsonProperty
  public int getMaxProcessingRecords()
  {
    return maxProcessingRecords;
  }

  @JsonProperty
  public long getMaxProcessingBytes()
  {
    return maxProcessingBytes;
  }

  @JsonProperty
  public long getClaimDurationMillis()
  {
    return claimDurationMillis;
  }

  @JsonProperty
  public long getClaimRenewalPeriodMillis()
  {
    return claimRenewalPeriodMillis;
  }

  @JsonProperty
  public long getInboxPollPeriodMillis()
  {
    return inboxPollPeriodMillis;
  }

  @Override
  public String toString()
  {
    return "ShareGroupIndexTaskIOConfig{" +
           "topic='" + topic + '\'' +
           ", groupId='" + groupId + '\'' +
           ", consumerProperties=" + consumerProperties +
           ", inputFormat=" + inputFormat +
           ", pollTimeout=" + pollTimeout +
           ", inboxId='" + inboxId + '\'' +
           ", inboxStorage=" + inboxStorage +
           ", receiptPageSize=" + receiptPageSize +
           ", maxStagingRecords=" + maxStagingRecords +
           ", maxStagingBytes=" + maxStagingBytes +
           ", maxConcurrentUploads=" + maxConcurrentUploads +
           ", renewalFraction=" + renewalFraction +
           ", maxProcessingManifests=" + maxProcessingManifests +
           ", maxProcessingRecords=" + maxProcessingRecords +
           ", maxProcessingBytes=" + maxProcessingBytes +
           ", claimDurationMillis=" + claimDurationMillis +
           ", claimRenewalPeriodMillis=" + claimRenewalPeriodMillis +
           ", inboxPollPeriodMillis=" + inboxPollPeriodMillis +
           '}';
  }
}
