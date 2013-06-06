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

package com.metamx.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.metamx.druid.indexing.coordinator.ForkingTaskRunner;
import com.metamx.emitter.service.ServiceEmitter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

/**
 */
public class WorkerServletModule extends JerseyServletModule
{
  private final ObjectMapper jsonMapper;
  private final ServiceEmitter emitter;
  private final ForkingTaskRunner forkingTaskRunner;

  public WorkerServletModule(
      ObjectMapper jsonMapper,
      ServiceEmitter emitter,
      ForkingTaskRunner forkingTaskRunner
  )
  {
    this.jsonMapper = jsonMapper;
    this.emitter = emitter;
    this.forkingTaskRunner = forkingTaskRunner;
  }

  @Override
  protected void configureServlets()
  {
    bind(WorkerResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(ServiceEmitter.class).toInstance(emitter);
    bind(ForkingTaskRunner.class).toInstance(forkingTaskRunner);

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
