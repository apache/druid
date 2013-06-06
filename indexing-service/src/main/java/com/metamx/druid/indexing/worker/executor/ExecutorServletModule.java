package com.metamx.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.metamx.druid.indexing.common.index.ChatHandlerProvider;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

public class ExecutorServletModule extends JerseyServletModule
{
  private final ObjectMapper jsonMapper;
  private final ChatHandlerProvider receivers;

  public ExecutorServletModule(
      ObjectMapper jsonMapper,
      ChatHandlerProvider receivers
  )
  {
    this.jsonMapper = jsonMapper;
    this.receivers = receivers;
  }

  @Override
  protected void configureServlets()
  {
    bind(ChatHandlerResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(ChatHandlerProvider.class).toInstance(receivers);

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
