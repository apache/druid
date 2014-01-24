/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.util.Providers;
import io.druid.client.DruidServerConfig;
import io.druid.guice.annotations.Self;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.loading.MMappedQueryableIndexFactory;
import io.druid.segment.loading.QueryableIndexFactory;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;

import javax.annotation.Nullable;

/**
 */
public class StorageNodeModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.server", DruidServerConfig.class);
    JsonConfigProvider.bind(binder, "druid.segmentCache", SegmentLoaderConfig.class);

    binder.bind(NodeTypeConfig.class).toProvider(Providers.<NodeTypeConfig>of(null));
    binder.bind(QueryableIndexFactory.class).to(MMappedQueryableIndexFactory.class).in(LazySingleton.class);

    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(DefaultQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public DruidServerMetadata getMetadata(@Self DruidNode node, @Nullable NodeTypeConfig nodeType, DruidServerConfig config)
  {
    if (nodeType == null) {
      throw new ProvisionException("Must override the binding for NodeTypeConfig if you want a DruidServerMetadata.");
    }

    return new DruidServerMetadata(
        node.getHost(),
        node.getHost(),
        config.getMaxSize(),
        nodeType.getNodeType(),
        config.getTier(),
        config.getPriority()
    );
  }
}
