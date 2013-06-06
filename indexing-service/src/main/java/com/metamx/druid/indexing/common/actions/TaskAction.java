package com.metamx.druid.indexing.common.actions;

import com.metamx.druid.indexing.common.task.Task;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "lockAcquire", value = LockAcquireAction.class),
    @JsonSubTypes.Type(name = "lockList", value = LockListAction.class),
    @JsonSubTypes.Type(name = "lockRelease", value = LockReleaseAction.class),
    @JsonSubTypes.Type(name = "segmentInsertion", value = SegmentInsertAction.class),
    @JsonSubTypes.Type(name = "segmentListUsed", value = SegmentListUsedAction.class),
    @JsonSubTypes.Type(name = "segmentListUnused", value = SegmentListUnusedAction.class),
    @JsonSubTypes.Type(name = "segmentNuke", value = SegmentNukeAction.class),
    @JsonSubTypes.Type(name = "spawnTasks", value = SpawnTasksAction.class)
})
public interface TaskAction<RetType>
{
  public TypeReference<RetType> getReturnTypeReference(); // T_T
  public RetType perform(Task task, TaskActionToolbox toolbox) throws IOException;
  public boolean isAudited();
}
