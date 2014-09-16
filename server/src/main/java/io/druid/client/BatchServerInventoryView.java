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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.ManageLifecycle;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 */
@ManageLifecycle
public class BatchServerInventoryView extends ServerInventoryView<Set<DataSegment>> implements FilteredServerView
{
  private static final EmittingLogger log = new EmittingLogger(BatchServerInventoryView.class);

  final private ConcurrentMap<String, Set<DataSegment>> zNodes = new MapMaker().makeMap();
  final private ConcurrentMap<SegmentCallback, Predicate<DataSegment>> segmentPredicates = new MapMaker().makeMap();
  final private Predicate<DataSegment> defaultFilter;

  @Inject
  public BatchServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper,
      final Predicate<DataSegment> defaultFilter
  )
  {
    super(
        log,
        zkPaths.getAnnouncementsPath(),
        zkPaths.getLiveSegmentsPath(),
        curator,
        jsonMapper,
        new TypeReference<Set<DataSegment>>(){}
    );

    Preconditions.checkNotNull(defaultFilter);
    this.defaultFilter = defaultFilter;
  }

  @Override
  protected DruidServer addInnerInventory(
      final DruidServer container,
      String inventoryKey,
      final Set<DataSegment> inventory
  )
  {
    Predicate<DataSegment> predicate = Predicates.or(defaultFilter, Predicates.or(segmentPredicates.values()));
    // make a copy of the set and not just a filtered view, in order to not keep all the segment data in memory
    Set<DataSegment> filteredInventory = Sets.newHashSet(Iterables.filter(inventory, predicate));

    zNodes.put(inventoryKey, filteredInventory);
    for (DataSegment segment : filteredInventory) {
      addSingleInventory(container, segment);
    }
    return container;
  }

  @Override
  protected DruidServer updateInnerInventory(
      DruidServer container, String inventoryKey, Set<DataSegment> inventory
  )
  {
    Predicate<DataSegment> predicate = Predicates.or(defaultFilter, Predicates.or(segmentPredicates.values()));
    // make a copy of the set and not just a filtered view, in order to not keep all the segment data in memory
    Set<DataSegment> filteredInventory = Sets.newHashSet(Iterables.filter(inventory, predicate));

    Set<DataSegment> existing = zNodes.get(inventoryKey);
    if (existing == null) {
      throw new ISE("Trying to update an inventoryKey[%s] that didn't exist?!", inventoryKey);
    }

    for (DataSegment segment : Sets.difference(filteredInventory, existing)) {
      addSingleInventory(container, segment);
    }
    for (DataSegment segment : Sets.difference(existing, filteredInventory)) {
      removeSingleInventory(container, segment.getIdentifier());
    }
    zNodes.put(inventoryKey, filteredInventory);

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

  @Override
  public void registerSegmentCallback(
      final Executor exec, final SegmentCallback callback, final Predicate<DataSegment> filter
  )
  {
    segmentPredicates.put(callback, filter);
    registerSegmentCallback(
        exec, new SegmentCallback()
        {
          @Override
          public CallbackAction segmentAdded(
              DruidServerMetadata server, DataSegment segment
          )
          {
            final CallbackAction action;
            if(filter.apply(segment)) {
              action = callback.segmentAdded(server, segment);
              if (action.equals(CallbackAction.UNREGISTER)) {
                segmentPredicates.remove(callback);
              }
            } else {
              action = CallbackAction.CONTINUE;
            }
            return action;
          }

          @Override
          public CallbackAction segmentRemoved(
              DruidServerMetadata server, DataSegment segment
          )
          {
            final CallbackAction action;
            if(filter.apply(segment)) {
              action = callback.segmentRemoved(server, segment);
              if (action.equals(CallbackAction.UNREGISTER)) {
                segmentPredicates.remove(callback);
              }
            } else {
              action = CallbackAction.CONTINUE;
            }
            return action;
          }

          @Override
          public CallbackAction segmentViewInitialized()
          {
            return callback.segmentViewInitialized();
          }
        }
    );
  }
}
