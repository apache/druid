package com.metamx.druid.http;

import com.google.inject.Provides;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.coordination.DruidClusterInfo;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.master.DruidMaster;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;

import javax.inject.Singleton;

/**
 */
public class MasterServletModule extends JerseyServletModule
{
  private final ServerInventoryManager serverInventoryManager;
  private final DatabaseSegmentManager segmentInventoryManager;
  private final DruidClusterInfo druidClusterInfo;
  private final DruidMaster master;
  private final ObjectMapper jsonMapper;

  public MasterServletModule(
      ServerInventoryManager serverInventoryManager,
      DatabaseSegmentManager segmentInventoryManager,
      DruidClusterInfo druidClusterInfo,
      DruidMaster master,
      ObjectMapper jsonMapper
  )
  {
    this.serverInventoryManager = serverInventoryManager;
    this.segmentInventoryManager = segmentInventoryManager;
    this.druidClusterInfo = druidClusterInfo;
    this.master = master;
    this.jsonMapper = jsonMapper;
  }

  @Override
  protected void configureServlets()
  {
    bind(InfoResource.class);
    bind(MasterResource.class);
    bind(ServerInventoryManager.class).toInstance(serverInventoryManager);
    bind(DatabaseSegmentManager.class).toInstance(segmentInventoryManager);
    bind(DruidMaster.class).toInstance(master);
    bind(DruidClusterInfo.class).toInstance(druidClusterInfo);

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
