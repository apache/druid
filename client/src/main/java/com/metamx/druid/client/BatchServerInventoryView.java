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
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.emitter.EmittingLogger;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 */
public class BatchServerInventoryView extends ServerInventoryView<Set<DataSegment>>
{
  private static final EmittingLogger log = new EmittingLogger(BatchServerInventoryView.class);

  final ConcurrentMap<String, Set<DataSegment>> zNodes = new MapMaker().makeMap();

  public BatchServerInventoryView(
      final ServerInventoryViewConfig config,
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ExecutorService exec,
      final ObjectMapper jsonMapper
  )
  {
    super(
        config, zkPaths, curator, exec, jsonMapper, new TypeReference<Set<DataSegment>>()
    {
    }
    );
  }

  @Override
  protected DruidServer addInnerInventory(
      final DruidServer container,
      String inventoryKey,
      final Set<DataSegment> inventory
  )
  {
    zNodes.put(inventoryKey, inventory);
    for (DataSegment segment : inventory) {
      addSingleInventory(container, segment);
    }
    return container;
  }

  @Override
  protected DruidServer updateInnerInventory(
      DruidServer container, String inventoryKey, Set<DataSegment> inventory
  )
  {
    Set<DataSegment> existing = zNodes.get(inventoryKey);
    if (existing == null) {
      throw new ISE("Trying to update an inventoryKey[%s] that didn't exist?!", inventoryKey);
    }

    for (DataSegment segment : Sets.difference(inventory, existing)) {
      addSingleInventory(container, segment);
    }
    for (DataSegment segment : Sets.difference(existing, inventory)) {
      removeSingleInventory(container, segment.getIdentifier());
    }

    return container;
  }

  @Override
  protected DruidServer removeInnerInventory(final DruidServer container, String inventoryKey)
  {
    log.info("Server[%s] removed container[%s]", container.getName(), inventoryKey);
    Set<DataSegment> segments = zNodes.remove(inventoryKey);

    if (segments == null) {
      log.warn("Told to remove container[%s], which didn't exist", inventoryKey);
      return container;
    }

    for (DataSegment segment : segments) {
      removeSingleInventory(container, segment.getIdentifier());
    }
    return container;
  }
}
