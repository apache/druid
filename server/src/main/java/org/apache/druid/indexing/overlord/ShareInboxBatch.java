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

import java.util.List;
import java.util.TreeSet;

public class ShareInboxBatch
{
  public static final int MAX_RECEIPT_PAGE_SIZE = 1_048_576;

  private final String dataSource;
  private final String inboxId;
  private final String groupId;
  private final String clusterId;
  private final String topicId;
  private final String topicName;
  private final int partitionId;
  private final String specFingerprint;
  private final String manifestId;
  private final String objectPath;
  private final String objectHash;
  private final long objectSize;
  private final List<Long> offsets;
  private final int receiptPageSize;

  @JsonCreator
  public ShareInboxBatch(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("inboxId") String inboxId,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("clusterId") String clusterId,
      @JsonProperty("topicId") String topicId,
      @JsonProperty("topicName") String topicName,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("specFingerprint") String specFingerprint,
      @JsonProperty("manifestId") String manifestId,
      @JsonProperty("objectPath") String objectPath,
      @JsonProperty("objectHash") String objectHash,
      @JsonProperty("objectSize") long objectSize,
      @JsonProperty("offsets") List<Long> offsets,
      @JsonProperty("receiptPageSize") int receiptPageSize
  )
  {
    this.dataSource = requireText(dataSource, "dataSource");
    this.inboxId = requireText(inboxId, "inboxId");
    this.groupId = requireText(groupId, "groupId");
    this.clusterId = requireText(clusterId, "clusterId");
    this.topicId = requireText(topicId, "topicId");
    this.topicName = requireText(topicName, "topicName");
    Preconditions.checkArgument(partitionId >= 0, "partitionId must not be negative");
    this.partitionId = partitionId;
    this.specFingerprint = requireText(specFingerprint, "specFingerprint");
    this.manifestId = requireText(manifestId, "manifestId");
    this.objectPath = requireText(objectPath, "objectPath");
    this.objectHash = requireText(objectHash, "objectHash");
    Preconditions.checkArgument(objectSize > 0, "objectSize must be positive");
    this.objectSize = objectSize;
    Preconditions.checkNotNull(offsets, "offsets");
    Preconditions.checkArgument(!offsets.isEmpty(), "offsets must not be empty");
    final TreeSet<Long> sortedOffsets = new TreeSet<>(offsets);
    Preconditions.checkArgument(sortedOffsets.size() == offsets.size(), "offsets must be unique");
    Preconditions.checkArgument(sortedOffsets.first() >= 0, "offsets must not be negative");
    this.offsets = List.copyOf(sortedOffsets);
    Preconditions.checkArgument(
        receiptPageSize > 0 && receiptPageSize <= MAX_RECEIPT_PAGE_SIZE,
        "receiptPageSize must be between 1 and %s",
        MAX_RECEIPT_PAGE_SIZE
    );
    this.receiptPageSize = receiptPageSize;
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
  public String getManifestId()
  {
    return manifestId;
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
  public List<Long> getOffsets()
  {
    return offsets;
  }

  @JsonProperty
  public int getReceiptPageSize()
  {
    return receiptPageSize;
  }

  private static String requireText(String value, String name)
  {
    Preconditions.checkNotNull(value, name);
    Preconditions.checkArgument(!value.isEmpty(), "%s must not be empty", name);
    return value;
  }
}
