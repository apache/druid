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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Provides;
import com.google.inject.util.Providers;
import com.metamx.druid.client.InventoryView;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.master.DruidMaster;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

/**
 */
public class MasterServletModule extends JerseyServletModule
{
  private final InventoryView serverInventoryView;
  private final DatabaseSegmentManager segmentInventoryManager;
  private final DatabaseRuleManager databaseRuleManager;
  private final DruidMaster master;
  private final ObjectMapper jsonMapper;
  private final IndexingServiceClient indexingServiceClient;

  public MasterServletModule(
      InventoryView serverInventoryView,
      DatabaseSegmentManager segmentInventoryManager,
      DatabaseRuleManager databaseRuleManager,
      DruidMaster master,
      ObjectMapper jsonMapper,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentInventoryManager = segmentInventoryManager;
    this.databaseRuleManager = databaseRuleManager;
    this.master = master;
    this.jsonMapper = jsonMapper;
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  protected void configureServlets()
  {
    bind(InfoResource.class);
    bind(MasterResource.class);
    bind(InventoryView.class).toInstance(serverInventoryView);
    bind(DatabaseSegmentManager.class).toInstance(segmentInventoryManager);
    bind(DatabaseRuleManager.class).toInstance(databaseRuleManager);
    bind(DruidMaster.class).toInstance(master);
    if (indexingServiceClient == null) {
      bind(IndexingServiceClient.class).toProvider(Providers.<IndexingServiceClient>of(null));
    }
    else {
      bind(IndexingServiceClient.class).toInstance(indexingServiceClient);
    }

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
