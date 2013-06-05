/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexing.coordinator.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.indexing.common.tasklogs.TaskLogProvider;
import com.metamx.druid.indexing.coordinator.TaskMasterLifecycle;
import com.metamx.druid.indexing.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.indexing.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.emitter.service.ServiceEmitter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

/**
 */
public class IndexerCoordinatorServletModule extends JerseyServletModule
{
  private final ObjectMapper jsonMapper;
  private final IndexerCoordinatorConfig indexerCoordinatorConfig;
  private final ServiceEmitter emitter;
  private final TaskMasterLifecycle taskMasterLifecycle;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final TaskLogProvider taskLogProvider;
  private final JacksonConfigManager configManager;

  public IndexerCoordinatorServletModule(
      ObjectMapper jsonMapper,
      IndexerCoordinatorConfig indexerCoordinatorConfig,
      ServiceEmitter emitter,
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogProvider taskLogProvider,
      JacksonConfigManager configManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.indexerCoordinatorConfig = indexerCoordinatorConfig;
    this.emitter = emitter;
    this.taskMasterLifecycle = taskMasterLifecycle;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogProvider = taskLogProvider;
    this.configManager = configManager;
  }

  @Override
  protected void configureServlets()
  {
    bind(IndexerCoordinatorResource.class);
    bind(OldIndexerCoordinatorResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(IndexerCoordinatorConfig.class).toInstance(indexerCoordinatorConfig);
    bind(ServiceEmitter.class).toInstance(emitter);
    bind(TaskMasterLifecycle.class).toInstance(taskMasterLifecycle);
    bind(TaskStorageQueryAdapter.class).toInstance(taskStorageQueryAdapter);
    bind(TaskLogProvider.class).toInstance(taskLogProvider);
    bind(JacksonConfigManager.class).toInstance(configManager);

    serve("/*").with(GuiceContainer.class);
  }

  @Provides
  @Singleton
  public JacksonJsonProvider getJacksonJsonProvider()
  {
    final JacksonJsonProvider provider = new JacksonJsonProvider();
    provider.setMapper(jsonMapper);
    return provider;
  }
}
