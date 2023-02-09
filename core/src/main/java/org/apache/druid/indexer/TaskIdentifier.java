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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Model class containing the id, type and groupId of a task
 * These fields are extracted from the task payload for the new schema and this model can be used for migration as well.
 */
public class TaskIdentifier
{

  private final String id;

  @Nullable
  private final String type;

  @Nullable
  private final String groupId;

  @JsonCreator
  public TaskIdentifier(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") @Nullable String groupId,
      @JsonProperty("type") @Nullable String type // nullable for backward compatibility
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = groupId;
    this.type = type;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Nullable
  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @Nullable
  @JsonProperty
  public String getType()
  {
    return type;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskIdentifier that = (TaskIdentifier) o;
    return Objects.equals(getId(), that.getId()) &&
           Objects.equals(getGroupId(), that.getGroupId()) &&
           Objects.equals(getType(), that.getType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getGroupId(),
        getType()
    );
  }

  @Override
  public String toString()
  {
    return "TaskIdentifier{" +
           "id='" + id + '\'' +
           ", groupId='" + groupId + '\'' +
           ", type='" + type + '\'' +
           '}';
  }
}
