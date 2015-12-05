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
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.ManageLifecycle;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 */
@ManageLifecycle
public class BatchServerInventoryView extends ServerInventoryView<Set<DataSegment>>
{
  private static final EmittingLogger log = new EmittingLogger(BatchServerInventoryView.class);

  final private ConcurrentMap<String, Set<DataSegment>> zNodes = new MapMaker().makeMap();

  @Inject
  public BatchServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper
  )
  {
    super(
        log,
        zkPaths.getAnnouncementsPath(),
        zkPaths.getLiveSegmentsPath(),
        curator,
        jsonMapper,
        new TypeReference<Set<DataSegment>>()
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
    zNodes.put(inventoryKey, inventory);

    return container;
  }

  @Override
  protected DruidServer removeInnerInventory(final DruidServer container, String inventoryKey)
  {
    log.debug("Server[%s] removed container[%s]", container.getName(), inventoryKey);
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
