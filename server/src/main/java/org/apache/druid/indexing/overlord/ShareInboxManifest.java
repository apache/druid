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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Objects;

public class ShareInboxManifest
{
  private final String manifestId;
  private final String dataSource;
  private final String inboxId;
  private final String groupId;
  private final String clusterId;
  private final String topicId;
  private final String topicName;
  private final int partitionId;
  private final String specFingerprint;
  private final String objectPath;
  private final String objectHash;
  private final long objectSize;
  private final List<Long> selectedOffsets;
  private final int recordCount;
  private final String claimOwner;
  private final long claimEpoch;
  private final DateTime claimExpiresAt;
  private final int processingAttempts;

  @JsonCreator
  public ShareInboxManifest(
      @JsonProperty("manifestId") String manifestId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("inboxId") String inboxId,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("clusterId") String clusterId,
      @JsonProperty("topicId") String topicId,
      @JsonProperty("topicName") String topicName,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("specFingerprint") String specFingerprint,
      @JsonProperty("objectPath") String objectPath,
      @JsonProperty("objectHash") String objectHash,
      @JsonProperty("objectSize") long objectSize,
      @JsonProperty("selectedOffsets") List<Long> selectedOffsets,
      @JsonProperty("recordCount") int recordCount,
      @JsonProperty("claimOwner") String claimOwner,
      @JsonProperty("claimEpoch") long claimEpoch,
      @JsonProperty("claimExpiresAt") DateTime claimExpiresAt,
      @JsonProperty("processingAttempts") int processingAttempts
  )
  {
    this.manifestId = Objects.requireNonNull(manifestId, "manifestId");
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.inboxId = Objects.requireNonNull(inboxId, "inboxId");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    this.topicId = Objects.requireNonNull(topicId, "topicId");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    Preconditions.checkArgument(partitionId >= 0, "partitionId must not be negative");
    this.partitionId = partitionId;
    this.specFingerprint = Objects.requireNonNull(specFingerprint, "specFingerprint");
    this.objectPath = Objects.requireNonNull(objectPath, "objectPath");
    this.objectHash = Objects.requireNonNull(objectHash, "objectHash");
    Preconditions.checkArgument(objectSize > 0, "objectSize must be positive");
    this.objectSize = objectSize;
    this.selectedOffsets = List.copyOf(selectedOffsets);
    Preconditions.checkArgument(recordCount > 0, "recordCount must be positive");
    Preconditions.checkArgument(recordCount == this.selectedOffsets.size(), "recordCount must match selectedOffsets");
    this.recordCount = recordCount;
    this.claimOwner = Objects.requireNonNull(claimOwner, "claimOwner");
    Preconditions.checkArgument(claimEpoch > 0, "claimEpoch must be positive");
    this.claimEpoch = claimEpoch;
    this.claimExpiresAt = Objects.requireNonNull(claimExpiresAt, "claimExpiresAt");
    Preconditions.checkArgument(processingAttempts > 0, "processingAttempts must be positive");
    this.processingAttempts = processingAttempts;
  }

  @JsonProperty
  public String getManifestId()
  {
    return manifestId;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getInboxId()
  {
    return inboxId;
  }

  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty
  public String getClusterId()
  {
    return clusterId;
  }

  @JsonProperty
  public String getTopicId()
  {
    return topicId;
  }

  @JsonProperty
  public String getTopicName()
  {
    return topicName;
  }

  @JsonProperty
  public int getPartitionId()
  {
    return partitionId;
  }

  @JsonProperty
  public String getSpecFingerprint()
  {
    return specFingerprint;
  }

  @JsonProperty
  public String getObjectPath()
  {
    return objectPath;
  }

  @JsonProperty
  public String getObjectHash()
  {
    return objectHash;
  }

  @JsonProperty
  public long getObjectSize()
  {
    return objectSize;
  }

  @JsonProperty
  public List<Long> getSelectedOffsets()
  {
    return selectedOffsets;
  }

  @JsonProperty
  public int getRecordCount()
  {
    return recordCount;
  }

  @JsonProperty
  public String getClaimOwner()
  {
    return claimOwner;
  }

  @JsonProperty
  public long getClaimEpoch()
  {
    return claimEpoch;
  }

  @JsonProperty
  public DateTime getClaimExpiresAt()
  {
    return claimExpiresAt;
  }

  @JsonProperty
  public int getProcessingAttempts()
  {
    return processingAttempts;
  }
}
