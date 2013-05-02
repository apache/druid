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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.curator.inventory.CuratorInventoryManager;
import com.metamx.druid.curator.inventory.CuratorInventoryManagerStrategy;
import com.metamx.druid.curator.inventory.InventoryManagerConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import com.metamx.emitter.EmittingLogger;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class ServerInventoryView implements ServerView, InventoryView
{
  private static final EmittingLogger log = new EmittingLogger(ServerInventoryView.class);

  private final CuratorInventoryManager<DruidServer, DataSegment> inventoryManager;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private final ConcurrentMap<ServerCallback, Executor> serverCallbacks = new MapMaker().makeMap();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new MapMaker().makeMap();

  private static final Map<String, Integer> removedSegments = new MapMaker().makeMap();

  public ServerInventoryView(
      final ServerInventoryViewConfig config,
      final ZkPathsConfig zkPaths,
      final CuratorFramework curator,
      final ExecutorService exec,
      final ObjectMapper jsonMapper
  )
  {
    inventoryManager = new CuratorInventoryManager<DruidServer, DataSegment>(
        curator,
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
        exec,
        new CuratorInventoryManagerStrategy<DruidServer, DataSegment>()
        {
          @Override
          public DruidServer deserializeContainer(byte[] bytes)
          {
            try {
              return jsonMapper.readValue(bytes, DruidServer.class);
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public byte[] serializeContainer(DruidServer container)
          {
            try {
              return jsonMapper.writeValueAsBytes(container);
            }
            catch (JsonProcessingException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public DataSegment deserializeInventory(byte[] bytes)
          {
            try {
              return jsonMapper.readValue(bytes, DataSegment.class);
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public byte[] serializeInventory(DataSegment inventory)
          {
            try {
              return jsonMapper.writeValueAsBytes(inventory);
            }
            catch (JsonProcessingException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void newContainer(DruidServer container)
          {
            log.info("New Server[%s]", container);
          }

          @Override
          public void deadContainer(DruidServer deadContainer)
          {
            log.info("Server Disdappeared[%s]", deadContainer);
            runServerCallbacks(deadContainer);
          }

          @Override
          public DruidServer updateContainer(DruidServer oldContainer, DruidServer newContainer)
          {
            return newContainer.addDataSegments(oldContainer);
          }

          @Override
          public DruidServer addInventory(final DruidServer container, String inventoryKey, final DataSegment inventory)
          {
            log.info("Server[%s] added segment[%s]", container.getName(), inventory);
            final DruidServer retVal = container.addDataSegment(inventoryKey, inventory);

            runSegmentCallbacks(
                new Function<SegmentCallback, CallbackAction>()
                {
                  @Override
                  public CallbackAction apply(SegmentCallback input)
                  {
                    return input.segmentAdded(container, inventory);
                  }
                }
            );

            return retVal;
          }

          @Override
          public DruidServer removeInventory(final DruidServer container, String inventoryKey)
          {
            log.info("Server[%s] removed segment[%s]", container.getName(), inventoryKey);
            final DataSegment segment = container.getSegment(inventoryKey);
            final DruidServer retVal = container.removeDataSegment(inventoryKey);

            runSegmentCallbacks(
                new Function<SegmentCallback, CallbackAction>()
                {
                  @Override
                  public CallbackAction apply(SegmentCallback input)
                  {
                    return input.segmentRemoved(container, segment);
                  }
                }
            );

            removedSegments.put(inventoryKey, config.getRemovedSegmentLifetime());
            return retVal;
          }
        }
    );
  }

  public int lookupSegmentLifetime(DataSegment segment)
  {
    Integer lifetime = removedSegments.get(segment.getIdentifier());
    return (lifetime == null) ? 0 : lifetime;
  }

  public void decrementRemovedSegmentsLifetime()
  {
    for (Iterator<Map.Entry<String, Integer>> mapIter = removedSegments.entrySet().iterator(); mapIter.hasNext(); ) {
      Map.Entry<String, Integer> segment = mapIter.next();
      int lifetime = segment.getValue() - 1;

      if (lifetime < 0) {
        mapIter.remove();
      } else {
        segment.setValue(lifetime);
      }
    }
  }

  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (started) {
      if (!started.get()) {
        inventoryManager.start();
        started.set(true);
      }
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (started) {
      if (started.getAndSet(false)) {
        inventoryManager.stop();
      }
    }
  }

  public boolean isStarted()
  {
    return started.get();
  }

  @Override
  public DruidServer getInventoryValue(String containerKey)
  {
    return inventoryManager.getInventoryValue(containerKey);
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    return inventoryManager.getInventory();
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  private void runSegmentCallbacks(
      final Function<SegmentCallback, CallbackAction> fn
  )
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
                segmentCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  private void runServerCallbacks(final DruidServer server)
  {
    for (final Map.Entry<ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == entry.getKey().serverRemoved(server)) {
                serverCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }
}
