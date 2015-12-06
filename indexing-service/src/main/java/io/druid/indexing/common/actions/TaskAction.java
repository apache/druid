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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexing.common.task.Task;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "lockAcquire", value = LockAcquireAction.class),
    @JsonSubTypes.Type(name = "lockTryAcquire", value = LockTryAcquireAction.class),
    @JsonSubTypes.Type(name = "lockList", value = LockListAction.class),
    @JsonSubTypes.Type(name = "lockRelease", value = LockReleaseAction.class),
    @JsonSubTypes.Type(name = "segmentInsertion", value = SegmentInsertAction.class),
    @JsonSubTypes.Type(name = "segmentListUsed", value = SegmentListUsedAction.class),
    @JsonSubTypes.Type(name = "segmentListUnused", value = SegmentListUnusedAction.class),
    @JsonSubTypes.Type(name = "segmentNuke", value = SegmentNukeAction.class),
    @JsonSubTypes.Type(name = "segmentMetadataUpdate", value = SegmentMetadataUpdateAction.class),
    @JsonSubTypes.Type(name = "segmentAllocate", value = SegmentAllocateAction.class)
})
public interface TaskAction<RetType>
{
  public TypeReference<RetType> getReturnTypeReference(); // T_T
  public RetType perform(Task task, TaskActionToolbox toolbox) throws IOException;
  public boolean isAudited();
}
