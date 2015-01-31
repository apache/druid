/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.MapMaker;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.ManageLifecycle;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 */
@ManageLifecycle
public class SingleServerInventoryView extends ServerInventoryView<DataSegment> implements FilteredServerView
{
  private static final EmittingLogger log = new EmittingLogger(SingleServerInventoryView.class);

  final private ConcurrentMap<SegmentCallback, Predicate<DataSegment>> segmentPredicates = new MapMaker().makeMap();
  private final Predicate<DataSegment> defaultFilter;

  @Inject
  public SingleServerInventoryView(
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ObjectMapper jsonMapper,
      final Predicate<DataSegment> defaultFilter
  )
  {
    super(
        log,
        zkPaths.getAnnouncementsPath(),
        zkPaths.getServedSegmentsPath(),
        curator,
        jsonMapper,
        new TypeReference<DataSegment>(){}
    );

    Preconditions.checkNotNull(defaultFilter);
    this.defaultFilter = defaultFilter;
  }

  @Override
  protected DruidServer addInnerInventory(
      DruidServer container, String inventoryKey, DataSegment inventory
  )
  {
    Predicate<DataSegment> predicate = Predicates.or(defaultFilter, Predicates.or(segmentPredicates.values()));
    if(predicate.apply(inventory)) {
      addSingleInventory(container, inventory);
    }
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
