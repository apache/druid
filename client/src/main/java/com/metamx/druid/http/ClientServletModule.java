package com.metamx.druid.http;

import com.google.inject.Provides;
import com.metamx.druid.client.ClientInventoryManager;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Singleton;

/**
 */
public class ClientServletModule extends JerseyServletModule
{
  private final QuerySegmentWalker texasRanger;
  private final ClientInventoryManager clientInventoryManager;
  private final ObjectMapper jsonMapper;

  public ClientServletModule(
      QuerySegmentWalker texasRanger,
      ClientInventoryManager clientInventoryManager,
      ObjectMapper jsonMapper
  )
  {
    this.texasRanger = texasRanger;
    this.clientInventoryManager = clientInventoryManager;
    this.jsonMapper = jsonMapper;
  }

  @Override
  protected void configureServlets()
  {
    bind(ClientInfoResource.class);
    bind(QuerySegmentWalker.class).toInstance(texasRanger);
    bind(ClientInventoryManager.class).toInstance(clientInventoryManager);

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

