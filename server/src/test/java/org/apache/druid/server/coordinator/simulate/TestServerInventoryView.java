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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentChangeHandler;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class TestServerInventoryView implements ServerInventoryView
{
  private static final Logger log = new Logger(TestServerInventoryView.class);

  private final ConcurrentHashMap<String, DruidServer> servers = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, DataSegmentChangeHandler> segmentChangeHandlers = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<SegmentCallback, Executor> segmentCallbacks = new ConcurrentHashMap<>();
  private final List<ServerChangeHandler> serverChangeHandlers = new ArrayList<>();

  public void setUp()
  {
    segmentCallbacks.forEach(
        (segmentCallback, executor) ->
            executor.execute(segmentCallback::segmentViewInitialized)
    );
  }

  /**
   * Synchronizes this inventory view with the given inventory view.
   */
  public void sync(ServerInventoryView other)
  {
    // Clear the current inventory
    for (ServerChangeHandler handler : serverChangeHandlers) {
      servers.values().forEach(handler::removeServer);
    }
    servers.clear();
    segmentChangeHandlers.clear();

    for (DruidServer server : other.getInventory()) {
      addServer(new DruidServer(
          server.getName(),
          server.getHostAndPort(),
          server.getHostAndTlsPort(),
          server.getMaxSize(),
          server.getType(),
          server.getTier(),
          server.getPriority()
      ));
      DataSegmentChangeHandler handler = getChangeHandlerForHost(server.getName());
      for (DataSegment segment : server.iterateAllSegments()) {
        handler.addSegment(segment, null);
      }
    }
  }

  public void addServer(DruidServer server)
  {
    servers.put(server.getName(), server);
    segmentChangeHandlers.put(server.getName(), new SegmentChangeHandler(server));
  }

  public void removeServer(DruidServer server)
  {
    servers.remove(server.getName());
    segmentChangeHandlers.remove(server.getName());

    for (ServerChangeHandler handler : serverChangeHandlers) {
      handler.removeServer(server);
    }
  }

  public DataSegmentChangeHandler getChangeHandlerForHost(String serverName)
  {
    return segmentChangeHandlers.get(serverName);
  }

  @Nullable
  @Override
  public DruidServer getInventoryValue(String serverKey)
  {
    return servers.get(serverKey);
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    return Collections.unmodifiableCollection(servers.values());
  }

  @Override
  public boolean isStarted()
  {
    return true;
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    DruidServer server = servers.get(serverKey);
    return server != null && server.getSegment(segment.getId()) != null;
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    serverChangeHandlers.add(new ServerChangeHandler(callback, exec));
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  private class SegmentChangeHandler implements DataSegmentChangeHandler
  {
    private final DruidServer server;

    private SegmentChangeHandler(DruidServer server)
    {
      this.server = server;
    }

    @Override
    public void addSegment(
        DataSegment segment,
        @Nullable DataSegmentChangeCallback callback
    )
    {
      log.debug("Adding segment [%s] to server [%s]", segment.getId(), server.getName());

      if (server.getMaxSize() - server.getCurrSize() >= segment.getSize()) {
        server.addDataSegment(segment);
        segmentCallbacks.forEach(
            (segmentCallback, executor) -> executor.execute(
                () -> segmentCallback.segmentAdded(server.getMetadata(), segment)
            )
        );
      } else {
        throw new ISE(
            "Not enough free space on server %s. Segment size [%d]. Current free space [%d]",
            server.getName(),
            segment.getSize(),
            server.getMaxSize() - server.getCurrSize()
        );
      }
    }

    @Override
    public void removeSegment(
        DataSegment segment,
        @Nullable DataSegmentChangeCallback callback
    )
    {
      log.debug("Removing segment [%s] from server [%s]", segment.getId(), server.getName());
      server.removeDataSegment(segment.getId());
      segmentCallbacks.forEach(
          (segmentCallback, executor) -> executor.execute(
              () -> segmentCallback.segmentAdded(server.getMetadata(), segment)
          )
      );
    }
  }

  private static class ServerChangeHandler
  {
    private final Executor executor;
    private final ServerRemovedCallback callback;

    private ServerChangeHandler(ServerRemovedCallback callback, Executor executor)
    {
      this.callback = callback;
      this.executor = executor;
    }

    private void removeServer(DruidServer server)
    {
      executor.execute(() -> callback.serverRemoved(server));
    }
  }
}
