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

import java.util.Map;
import java.util.TreeMap;

public class ShareInboxCompletionRequest
{
  private final String dataSource;
  private final String inboxId;
  private final String specFingerprint;
  private final String taskId;
  private final Map<String, Long> claims;
  private final String completionId;

  @JsonCreator
  public ShareInboxCompletionRequest(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("inboxId") String inboxId,
      @JsonProperty("specFingerprint") String specFingerprint,
      @JsonProperty("taskId") String taskId,
      @JsonProperty("claims") Map<String, Long> claims,
      @JsonProperty("completionId") String completionId
  )
  {
    this.dataSource = requireText(dataSource, "dataSource");
    this.inboxId = requireText(inboxId, "inboxId");
    this.specFingerprint = requireText(specFingerprint, "specFingerprint");
    this.taskId = requireText(taskId, "taskId");
    Preconditions.checkNotNull(claims, "claims");
    Preconditions.checkArgument(!claims.isEmpty(), "claims must not be empty");
    final TreeMap<String, Long> sortedClaims = new TreeMap<>();
    for (Map.Entry<String, Long> claim : claims.entrySet()) {
      sortedClaims.put(requireText(claim.getKey(), "manifestId"), claim.getValue());
      Preconditions.checkArgument(claim.getValue() != null && claim.getValue() > 0, "claim epoch must be positive");
    }
    this.claims = Map.copyOf(sortedClaims);
    this.completionId = requireText(completionId, "completionId");
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
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public Map<String, Long> getClaims()
  {
    return claims;
  }

  @JsonProperty
  public String getCompletionId()
  {
    return completionId;
  }

  private static String requireText(String value, String name)
  {
    Preconditions.checkNotNull(value, name);
    Preconditions.checkArgument(!value.isEmpty(), "%s must not be empty", name);
    return value;
  }
}
