package com.metamx.druid.merger.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.metamx.druid.merger.common.index.EventReceiverProvider;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

public class ExecutorServletModule extends JerseyServletModule
{
  private final ObjectMapper jsonMapper;
  private final EventReceiverProvider receivers;

  public ExecutorServletModule(
      ObjectMapper jsonMapper,
      EventReceiverProvider receivers
  )
  {
    this.jsonMapper = jsonMapper;
    this.receivers = receivers;
  }

  @Override
  protected void configureServlets()
  {
    bind(EventReceiverResource.class);
    bind(ObjectMapper.class).toInstance(jsonMapper);
    bind(EventReceiverProvider.class).toInstance(receivers);

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
