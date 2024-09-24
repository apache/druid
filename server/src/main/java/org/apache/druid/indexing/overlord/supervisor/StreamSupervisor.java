package org.apache.druid.indexing.overlord.supervisor;

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;

import java.util.List;

public interface StreamSupervisor extends Supervisor
{
  /**
   * Resets all offsets for a dataSource.
   * @param dataSourceMetadata optional dataSource metadata.
   */
  void reset(DataSourceMetadata dataSourceMetadata);

  /**
   * Reset offsets with provided dataSource metadata. The resulting stored offsets should be a union of existing checkpointed
   * offsets with provided offsets.
   * @param resetDataSourceMetadata required datasource metadata with offsets to reset.
   * @throws DruidException if any metadata attribute doesn't match the supervisor's state.
   */
  void resetOffsets(DataSourceMetadata resetDataSourceMetadata);

  /**
   * The definition of checkpoint is not very strict as currently it does not affect data or control path.
   * On this call Supervisor can potentially checkpoint data processed so far to some durable storage
   * for example - Kafka/Kinesis Supervisor uses this to merge and handoff segments containing at least the data
   * represented by {@param currentCheckpoint} DataSourceMetadata
   *
   * @param taskGroupId        unique Identifier to figure out for which sequence to do checkpointing
   * @param checkpointMetadata metadata for the sequence to currently checkpoint
   */
  void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata);

  /**
   * Computes maxLag, totalLag and avgLag
   */
  LagStats computeLagStats();

  int getActiveTaskGroupsCount();

  /**
   * Marks the given task groups as ready for segment hand-off irrespective of the task run times.
   * In the subsequent run, the supervisor initiates segment publish and hand-off for these task groups and rolls over their tasks.
   * taskGroupIds that are not valid or not actively reading are simply ignored.
   */
  default void handoffTaskGroupsEarly(List<Integer> taskGroupIds)
  {
    throw new UnsupportedOperationException("Supervisor does not have the feature to handoff task groups early implemented");
  }
}
