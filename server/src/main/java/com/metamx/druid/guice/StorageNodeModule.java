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

package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.coordination.DruidServerMetadata;
import com.metamx.druid.guice.annotations.Self;
import com.metamx.druid.initialization.DruidNode;
import com.metamx.druid.loading.MMappedQueryableIndexFactory;
import com.metamx.druid.loading.QueryableIndexFactory;
import com.metamx.druid.loading.SegmentLoaderConfig;
import com.metamx.druid.query.DefaultQueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;

/**
 */
public class StorageNodeModule extends ServerModule
{
  private final String nodeType;

  public StorageNodeModule(String nodeType)
  {
    this.nodeType = nodeType;
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    JsonConfigProvider.bind(binder, "druid.server", DruidServerConfig.class);
    JsonConfigProvider.bind(binder, "druid.segmentCache", SegmentLoaderConfig.class);

    binder.bind(QueryableIndexFactory.class).to(MMappedQueryableIndexFactory.class).in(LazySingleton.class);

    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(DefaultQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public DruidServerMetadata getMetadata(@Self DruidNode node, DruidServerConfig config)
  {
    return new DruidServerMetadata(
        node.getHost(),
        node.getHost(),
        config.getMaxSize(),
        nodeType,
        config.getTier()
    );
  }
}
