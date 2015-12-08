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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.ManageLifecycle;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

/**
 */
@ManageLifecycle
public class SingleServerInventoryView extends ServerInventoryView<DataSegment>
{
  private static final EmittingLogger log = new EmittingLogger(SingleServerInventoryView.class);

  @Inject
  public SingleServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper
  )
  {
    super(
        log,
        zkPaths.getAnnouncementsPath(),
        zkPaths.getServedSegmentsPath(),
        curator,
        jsonMapper,
        new TypeReference<DataSegment>()
        {
        }
    );
  }

  @Override
  protected DruidServer addInnerInventory(
      DruidServer container, String inventoryKey, DataSegment inventory
  )
  {
    addSingleInventory(container, inventory);
    return container;
  }

  @Override
  protected DruidServer updateInnerInventory(
      DruidServer container, String inventoryKey, DataSegment inventory
  )
  {
    return addInnerInventory(container, inventoryKey, inventory);
  }

  @Override
  protected DruidServer removeInnerInventory(DruidServer container, String inventoryKey)
  {
    removeSingleInventory(container, inventoryKey);
    return container;
  }
}
