/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.timeline.DataSegment;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * This class is deprecated. Use {@link HttpServerInventoryView} instead.
 */
@Deprecated
@ManageLifecycle
public class SingleServerInventoryView extends AbstractCuratorServerInventoryView<DataSegment> implements FilteredServerInventoryView
{
  private static final EmittingLogger log = new EmittingLogger(SingleServerInventoryView.class);

  private final ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates =
      new ConcurrentHashMap<>();
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;

  @Inject
  public SingleServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter
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

    Preconditions.checkNotNull(defaultFilter);
    this.defaultFilter = defaultFilter;
  }

  @Override
  protected DruidServer addInnerInventory(DruidServer container, String inventoryKey, DataSegment inventory)
  {
    Predicate<Pair<DruidServerMetadata, DataSegment>> predicate = Predicates.or(
        defaultFilter,
        Predicates.or(segmentPredicates.values())
    );
    if (predicate.apply(Pair.of(container.getMetadata(), inventory))) {
      addSingleInventory(container, DataSegmentInterner.intern(inventory));
    }
    return container;
  }

  @Override
  protected DruidServer updateInnerInventory(DruidServer container, String inventoryKey, DataSegment inventory)
  {
    return addInnerInventory(container, inventoryKey, inventory);
  }

  @Override
  protected DruidServer removeInnerInventory(DruidServer container, String inventoryKey)
  {
    removeSingleInventory(container, inventoryKey);
    return container;
  }

  @Override
  public void registerSegmentCallback(
      final Executor exec,
      final SegmentCallback callback,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    SegmentCallback filteringCallback = new SingleServerInventoryView.FilteringSegmentCallback(callback, filter);
    segmentPredicates.put(filteringCallback, filter);
    registerSegmentCallback(
        exec,
        filteringCallback
    );
  }

  @Override
  protected void segmentCallbackRemoved(SegmentCallback callback)
  {
    segmentPredicates.remove(callback);
  }

  static class FilteringSegmentCallback implements SegmentCallback
  {

    private final SegmentCallback callback;
    private final Predicate<Pair<DruidServerMetadata, DataSegment>> filter;

    FilteringSegmentCallback(SegmentCallback callback, Predicate<Pair<DruidServerMetadata, DataSegment>> filter)
    {
      this.callback = callback;
      this.filter = filter;
    }

    @Override
    public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      final CallbackAction action;
      if (filter.apply(Pair.of(server, segment))) {
        action = callback.segmentAdded(server, segment);
      } else {
        action = CallbackAction.CONTINUE;
      }
      return action;
    }

    @Override
    public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      final CallbackAction action;
      if (filter.apply(Pair.of(server, segment))) {
        action = callback.segmentRemoved(server, segment);
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

}
