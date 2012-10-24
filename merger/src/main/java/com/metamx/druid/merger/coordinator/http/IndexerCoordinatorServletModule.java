package com.metamx.druid.merger.coordinator.http;

import com.google.inject.Provides;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
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

  public IndexerCoordinatorServletModule(
      ObjectMapper jsonMapper,
      IndexerCoordinatorConfig indexerCoordinatorConfig,
      ServiceEmitter emitter,
      TaskQueue tasks
  )
  {
    this.jsonMapper = jsonMapper;
    this.indexerCoordinatorConfig = indexerCoordinatorConfig;
    this.emitter = emitter;
    this.tasks = tasks;
  }

  @Override
  protected void configureServlets()
  {
    bind(IndexerCoordinatorResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(IndexerCoordinatorConfig.class).toInstance(indexerCoordinatorConfig);
    bind(ServiceEmitter.class).toInstance(emitter);
    bind(TaskQueue.class).toInstance(tasks);

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
