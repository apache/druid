package com.metamx.druid.merger.common.actions;

import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.type.TypeReference;

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
  public Task getTask(); // TODO Look into replacing this with task ID so stuff serializes smaller
  public TypeReference<RetType> getReturnTypeReference(); // T_T
  public RetType perform(TaskActionToolbox toolbox);
}
