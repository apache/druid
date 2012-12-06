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

package com.metamx.druid.http;

import com.google.inject.Provides;
import com.metamx.druid.client.ServerInventoryManager;
import com.metamx.druid.coordination.DruidClusterInfo;
import com.metamx.druid.db.DatabaseRuleManager;
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
  private final DatabaseRuleManager databaseRuleManager;
  private final DruidClusterInfo druidClusterInfo;
  private final DruidMaster master;
  private final ObjectMapper jsonMapper;

  public MasterServletModule(
      ServerInventoryManager serverInventoryManager,
      DatabaseSegmentManager segmentInventoryManager,
      DatabaseRuleManager databaseRuleManager,
      DruidClusterInfo druidClusterInfo,
      DruidMaster master,
      ObjectMapper jsonMapper
  )
  {
    this.serverInventoryManager = serverInventoryManager;
    this.segmentInventoryManager = segmentInventoryManager;
    this.databaseRuleManager = databaseRuleManager;
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
    bind(DatabaseRuleManager.class).toInstance(databaseRuleManager);
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
