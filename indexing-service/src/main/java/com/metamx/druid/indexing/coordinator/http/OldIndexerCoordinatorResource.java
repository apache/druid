package com.metamx.druid.indexing.coordinator.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.indexing.common.tasklogs.TaskLogProvider;
import com.metamx.druid.indexing.coordinator.TaskMasterLifecycle;
import com.metamx.druid.indexing.coordinator.TaskStorageQueryAdapter;

import javax.ws.rs.Path;

/**
 */
@Deprecated
@Path("/mmx/merger/v1")
public class OldIndexerCoordinatorResource extends IndexerCoordinatorResource
{
  @Inject
  public OldIndexerCoordinatorResource(
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogProvider taskLogProvider,
      JacksonConfigManager configManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    super(taskMasterLifecycle, taskStorageQueryAdapter, taskLogProvider, configManager, jsonMapper);
  }
}
