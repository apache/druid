/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import com.google.inject.util.Providers;
import io.druid.client.DruidServerConfig;
import io.druid.guice.annotations.Self;
import io.druid.query.DruidProcessingConfig;
import io.druid.segment.column.ColumnConfig;
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
    binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
  }

  @Provides
  @LazySingleton
  public DruidServerMetadata getMetadata(@Self DruidNode node, @Nullable NodeTypeConfig nodeType, DruidServerConfig config)
  {
    if (nodeType == null) {
      throw new ProvisionException("Must override the binding for NodeTypeConfig if you want a DruidServerMetadata.");
    }

    return new DruidServerMetadata(
        node.getHostAndPortToUse(),
        node.getHostAndPort(),
        node.getHostAndTlsPort(),
        config.getMaxSize(),
        nodeType.getNodeType(),
        config.getTier(),
        config.getPriority()
    );
  }
}
