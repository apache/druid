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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;

import java.util.concurrent.Future;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = TaskAction.TYPE_FIELD)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "lockAcquire", value = TimeChunkLockAcquireAction.class),
    @JsonSubTypes.Type(name = "lockTryAcquire", value = TimeChunkLockTryAcquireAction.class),
    @JsonSubTypes.Type(name = "segmentLockTryAcquire", value = SegmentLockTryAcquireAction.class),
    @JsonSubTypes.Type(name = "segmentLockAcquire", value = SegmentLockAcquireAction.class),
    @JsonSubTypes.Type(name = "lockList", value = LockListAction.class),
    @JsonSubTypes.Type(name = "lockRelease", value = LockReleaseAction.class),
    @JsonSubTypes.Type(name = "segmentInsertion", value = SegmentInsertAction.class),
    @JsonSubTypes.Type(name = "segmentTransactionalInsert", value = SegmentTransactionalInsertAction.class),
    @JsonSubTypes.Type(name = "segmentTransactionalAppend", value = SegmentTransactionalAppendAction.class),
    @JsonSubTypes.Type(name = "segmentTransactionalReplace", value = SegmentTransactionalReplaceAction.class),
    // Type name doesn't correspond to the name of the class for backward compatibility.
    @JsonSubTypes.Type(name = "segmentListUsed", value = RetrieveUsedSegmentsAction.class),
    // Type name doesn't correspond to the name of the class for backward compatibility.
    @JsonSubTypes.Type(name = "segmentListUnused", value = RetrieveUnusedSegmentsAction.class),
    @JsonSubTypes.Type(name = "markSegmentsAsUnused", value = MarkSegmentsAsUnusedAction.class),
    @JsonSubTypes.Type(name = "segmentNuke", value = SegmentNukeAction.class),
    @JsonSubTypes.Type(name = "segmentMetadataUpdate", value = SegmentMetadataUpdateAction.class),
    @JsonSubTypes.Type(name = SegmentAllocateAction.TYPE, value = SegmentAllocateAction.class),
    @JsonSubTypes.Type(name = "resetDataSourceMetadata", value = ResetDataSourceMetadataAction.class),
    @JsonSubTypes.Type(name = "checkPointDataSourceMetadata", value = CheckPointDataSourceMetadataAction.class),
    @JsonSubTypes.Type(name = "surrogateAction", value = SurrogateAction.class),
    @JsonSubTypes.Type(name = "updateStatus", value = UpdateStatusAction.class),
    @JsonSubTypes.Type(name = "updateLocation", value = UpdateLocationAction.class)
})
public interface TaskAction<RetType>
{
  String TYPE_FIELD = "type";

  TypeReference<RetType> getReturnTypeReference(); // T_T

  RetType perform(Task task, TaskActionToolbox toolbox);

  boolean isAudited();

  default boolean canPerformAsync(Task task, TaskActionToolbox toolbox)
  {
    return false;
  }

  default Future<RetType> performAsync(Task task, TaskActionToolbox toolbox)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  String toString();
}
