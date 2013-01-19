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

package com.metamx.druid.merger.coordinator.http;

import com.google.inject.Provides;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.emitter.service.ServiceEmitter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Singleton;

/**
 */
public class IndexerCoordinatorServletModule extends JerseyServletModule
{
  private final ObjectMapper jsonMapper;
  private final IndexerCoordinatorConfig indexerCoordinatorConfig;
  private final ServiceEmitter emitter;
  private final TaskQueue tasks;
  private final WorkerSetupManager workerSetupManager;

  public IndexerCoordinatorServletModule(
      ObjectMapper jsonMapper,
      IndexerCoordinatorConfig indexerCoordinatorConfig,
      ServiceEmitter emitter,
      TaskQueue tasks,
      WorkerSetupManager workerSetupManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.indexerCoordinatorConfig = indexerCoordinatorConfig;
    this.emitter = emitter;
    this.tasks = tasks;
    this.workerSetupManager = workerSetupManager;
  }

  @Override
  protected void configureServlets()
  {
    bind(IndexerCoordinatorResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(IndexerCoordinatorConfig.class).toInstance(indexerCoordinatorConfig);
    bind(ServiceEmitter.class).toInstance(emitter);
    bind(TaskQueue.class).toInstance(tasks);
    bind(WorkerSetupManager.class).toInstance(workerSetupManager);

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
