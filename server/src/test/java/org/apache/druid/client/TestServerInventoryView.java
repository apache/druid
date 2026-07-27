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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * In-memory test fake for {@link ServerInventoryView} / {@link FilteredServerInventoryView}.
 *
 * Tests drive it directly via {@link #addServer}, {@link #removeServer}, {@link #addSegment},
 * {@link #removeSegment}, and {@link #markInventoryInitialized} instead of going through
 * ZooKeeper. Each driver method synchronously dispatches the corresponding callback.
 */
public class TestServerInventoryView implements ServerInventoryView, FilteredServerInventoryView
{
  private final ConcurrentHashMap<String, DruidServer> inventory = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ServerCallback, Executor> serverCallbacks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<SegmentCallback, Executor> segmentCallbacks = new ConcurrentHashMap<>();

  private volatile boolean started = false;

  public void start()
  {
    started = true;
  }

  public void stop()
  {
    started = false;
  }

  public void addServer(DruidServer server)
  {
    if (inventory.putIfAbsent(server.getName(), server) != null) {
      return;
    }
    runServerCallbacks(callback -> callback.serverAdded(server));
  }

  public void removeServer(DruidServer server)
  {
    inventory.remove(server.getName());
    runServerCallbacks(callback -> callback.serverRemoved(server));
  }

  public void addSegment(DruidServer server, DataSegment segment)
  {
    server.addDataSegment(segment);
    runSegmentCallbacks(callback -> callback.segmentAdded(server.getMetadata(), segment));
  }

  public void removeSegment(DruidServer server, DataSegment segment)
  {
    server.removeDataSegment(segment.getId());
    runSegmentCallbacks(callback -> callback.segmentRemoved(server.getMetadata(), segment));
  }

  public void markInventoryInitialized()
  {
    runSegmentCallbacks(SegmentCallback::segmentViewInitialized);
  }

  @Override
  public DruidServer getInventoryValue(String serverKey)
  {
    return inventory.get(serverKey);
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    return inventory.values();
  }

  @Override
  public boolean isStarted()
  {
    return started;
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    DruidServer server = getInventoryValue(serverKey);
    return server != null && server.getSegment(segment.getId()) != null;
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

  @Override
  public void registerSegmentCallback(
      Executor exec,
      SegmentCallback callback,
      Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    registerSegmentCallback(exec, new FilteringSegmentCallback(callback, filter));
  }

  protected void runServerCallbacks(final Function<ServerCallback, CallbackAction> fn)
  {
    for (final Map.Entry<ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(() -> {
        if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
          serverCallbacks.remove(entry.getKey());
        }
      });
    }
  }

  protected void runSegmentCallbacks(final Function<SegmentCallback, CallbackAction> fn)
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(() -> {
        if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
          segmentCallbacks.remove(entry.getKey());
        }
      });
    }
  }
}
