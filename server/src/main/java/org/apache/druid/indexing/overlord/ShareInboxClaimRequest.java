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

public class ShareInboxClaimRequest
{
  private final String dataSource;
  private final String inboxId;
  private final String specFingerprint;
  private final String claimOwner;
  private final int maxManifests;
  private final int maxRecords;
  private final long maxBytes;
  private final long claimDurationMillis;

  @JsonCreator
  public ShareInboxClaimRequest(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("inboxId") String inboxId,
      @JsonProperty("specFingerprint") String specFingerprint,
      @JsonProperty("claimOwner") String claimOwner,
      @JsonProperty("maxManifests") int maxManifests,
      @JsonProperty("maxRecords") int maxRecords,
      @JsonProperty("maxBytes") long maxBytes,
      @JsonProperty("claimDurationMillis") long claimDurationMillis
  )
  {
    this.dataSource = requireText(dataSource, "dataSource");
    this.inboxId = requireText(inboxId, "inboxId");
    this.specFingerprint = requireText(specFingerprint, "specFingerprint");
    this.claimOwner = requireText(claimOwner, "claimOwner");
    Preconditions.checkArgument(maxManifests > 0, "maxManifests must be positive");
    Preconditions.checkArgument(maxRecords > 0, "maxRecords must be positive");
    Preconditions.checkArgument(maxBytes > 0, "maxBytes must be positive");
    Preconditions.checkArgument(claimDurationMillis > 0, "claimDurationMillis must be positive");
    this.maxManifests = maxManifests;
    this.maxRecords = maxRecords;
    this.maxBytes = maxBytes;
    this.claimDurationMillis = claimDurationMillis;
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
  public String getSpecFingerprint()
  {
    return specFingerprint;
  }

  @JsonProperty
  public String getClaimOwner()
  {
    return claimOwner;
  }

  @JsonProperty
  public int getMaxManifests()
  {
    return maxManifests;
  }

  @JsonProperty
  public int getMaxRecords()
  {
    return maxRecords;
  }

  @JsonProperty
  public long getMaxBytes()
  {
    return maxBytes;
  }

  @JsonProperty
  public long getClaimDurationMillis()
  {
    return claimDurationMillis;
  }

  private static String requireText(String value, String name)
  {
    Preconditions.checkNotNull(value, name);
    Preconditions.checkArgument(!value.isEmpty(), "%s must not be empty", name);
    return value;
  }
}
