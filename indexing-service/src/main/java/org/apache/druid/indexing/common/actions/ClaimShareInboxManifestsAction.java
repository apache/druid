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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ShareInboxClaimRequest;
import org.apache.druid.indexing.overlord.ShareInboxClaimResult;

import java.util.Objects;

public class ClaimShareInboxManifestsAction implements TaskAction<ShareInboxClaimResult>
{
  public static final String TYPE = "claimShareInboxManifests";

  private final ShareInboxClaimRequest request;

  @JsonCreator
  public ClaimShareInboxManifestsAction(@JsonProperty("request") ShareInboxClaimRequest request)
  {
    this.request = Objects.requireNonNull(request, "request");
  }

  @JsonProperty
  public ShareInboxClaimRequest getRequest()
  {
    return request;
  }

  @Override
  public TypeReference<ShareInboxClaimResult> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public ShareInboxClaimResult perform(Task task, TaskActionToolbox toolbox)
  {
    validateTask(task, request.getDataSource(), request.getClaimOwner());
    return toolbox.getIndexerMetadataStorageCoordinator().claimShareInboxManifests(request);
  }

  static void validateTask(Task task, String dataSource, String claimOwner)
  {
    if (!task.getDataSource().equals(dataSource) || !task.getId().equals(claimOwner)) {
      throw InvalidInput.exception(
          "Task[%s] cannot manage share inbox claims for datasource[%s] as owner[%s]",
          task.getId(),
          dataSource,
          claimOwner
      );
    }
  }

  @Override
  public String toString()
  {
    return "ClaimShareInboxManifestsAction{" +
           "request=" + request +
           '}';
  }
}
