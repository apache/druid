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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ShareInboxRenewRequest;
import org.apache.druid.indexing.overlord.ShareInboxRenewResult;

import java.util.Objects;

public class RenewShareInboxClaimsAction implements TaskAction<ShareInboxRenewResult>
{
  public static final String TYPE = "renewShareInboxClaims";

  private final ShareInboxRenewRequest request;

  @JsonCreator
  public RenewShareInboxClaimsAction(@JsonProperty("request") ShareInboxRenewRequest request)
  {
    this.request = Objects.requireNonNull(request, "request");
  }

  @JsonProperty
  public ShareInboxRenewRequest getRequest()
  {
    return request;
  }

  @Override
  public TypeReference<ShareInboxRenewResult> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public ShareInboxRenewResult perform(Task task, TaskActionToolbox toolbox)
  {
    ClaimShareInboxManifestsAction.validateTask(task, request.getDataSource(), request.getClaimOwner());
    return toolbox.getIndexerMetadataStorageCoordinator().renewShareInboxClaims(request);
  }

  @Override
  public String toString()
  {
    return "RenewShareInboxClaimsAction{" +
           "request=" + request +
           '}';
  }
}
