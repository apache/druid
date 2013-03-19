package com.metamx.druid.merger.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.metamx.druid.merger.coordinator.ForkingTaskRunner;
import com.metamx.druid.merger.coordinator.http.IndexerCoordinatorResource;
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
