package com.metamx.druid.indexing.coordinator.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.indexing.common.tasklogs.TaskLogProvider;
import com.metamx.druid.indexing.coordinator.TaskMasterLifecycle;
import com.metamx.druid.indexing.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.indexing.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.emitter.service.ServiceEmitter;

import javax.ws.rs.Path;

/**
 */
@Deprecated
@Path("/mmx/merger/v1")
public class OldIndexerCoordinatorResource extends IndexerCoordinatorResource
{
  @Inject
  public OldIndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogProvider taskLogProvider,
      JacksonConfigManager configManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    super(config, emitter, taskMasterLifecycle, taskStorageQueryAdapter, taskLogProvider, configManager, jsonMapper);
  }
}
