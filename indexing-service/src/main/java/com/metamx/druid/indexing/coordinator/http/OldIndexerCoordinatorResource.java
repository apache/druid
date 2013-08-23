package com.metamx.druid.indexing.coordinator.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.indexing.common.tasklogs.TaskLogStreamer;
import com.metamx.druid.indexing.coordinator.TaskMaster;
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
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    super(taskMaster, taskStorageQueryAdapter, taskLogStreamer, configManager, jsonMapper);
  }
}
