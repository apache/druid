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
import com.metamx.druid.client.ServerInventoryView;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import javax.inject.Singleton;

/**
 */
public class ClientServletModule extends JerseyServletModule
{
  private final QuerySegmentWalker texasRanger;
  private final ServerInventoryView serverInventoryView;
  private final ObjectMapper jsonMapper;

  public ClientServletModule(
      QuerySegmentWalker texasRanger,
      ServerInventoryView serverInventoryView,
      ObjectMapper jsonMapper
  )
  {
    this.texasRanger = texasRanger;
    this.serverInventoryView = serverInventoryView;
    this.jsonMapper = jsonMapper;
  }

  @Override
  protected void configureServlets()
  {
    bind(ClientInfoResource.class);
    bind(QuerySegmentWalker.class).toInstance(texasRanger);
    bind(ServerInventoryView.class).toInstance(serverInventoryView);

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

