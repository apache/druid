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

package com.metamx.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.curator.inventory.InventoryManagerConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.emitter.EmittingLogger;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ExecutorService;

/**
 */
public class SingleServerInventoryView extends ServerInventoryView<DataSegment>
{
  private static final EmittingLogger log = new EmittingLogger(SingleServerInventoryView.class);

  public SingleServerInventoryView(
      final ServerInventoryViewConfig config,
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ExecutorService exec,
      final ObjectMapper jsonMapper
  )
  {
    super(
        config,
        log,
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return zkPaths.getAnnouncementsPath();
          }

          @Override
          public String getInventoryPath()
          {
            return zkPaths.getServedSegmentsPath();
          }
        },
        curator,
        exec,
        jsonMapper,
        new TypeReference<DataSegment>(){}
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
