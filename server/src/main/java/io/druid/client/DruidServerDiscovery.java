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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.curator.inventory.CuratorInventoryManager;
import io.druid.curator.inventory.CuratorInventoryManagerStrategy;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.java.util.common.ISE;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

/**
 * Discovers DruidServer instances that serve segments using CuratorInventoryManager.
 */
public class DruidServerDiscovery
{
  private final EmittingLogger log = new EmittingLogger(DruidServerDiscovery.class);
  private final CuratorInventoryManager curatorInventoryManager;
  private volatile Listener listener;

  DruidServerDiscovery(
      final CuratorFramework curatorFramework,
      final String announcementsPath,
      final ObjectMapper jsonMapper
  )
  {
    curatorInventoryManager = initCuratorInventoryManager(curatorFramework, announcementsPath, jsonMapper);
  }

  public void start() throws Exception
  {
    Preconditions.checkNotNull(listener, "listener is not configured yet");
    curatorInventoryManager.start();
  }

  public void stop() throws IOException
  {
    curatorInventoryManager.stop();
  }

  private CuratorInventoryManager initCuratorInventoryManager(
      final CuratorFramework curator,
      final String announcementsPath,
      final ObjectMapper jsonMapper
  )
  {
    return new CuratorInventoryManager<>(
        curator,
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return announcementsPath;
          }

          @Override
          public String getInventoryPath()
          {
            return "/NON_EXISTENT_DUMMY_INVENTORY_PATH";
          }
        },
        Execs.singleThreaded("CuratorInventoryManagerBasedServerDiscovery-%s"),
        new CuratorInventoryManagerStrategy<DruidServer, Object>()
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
          public void newContainer(DruidServer container)
          {
            log.info("New Server[%s]", container.getName());
            listener.serverAdded(container);
          }

          @Override
          public void deadContainer(DruidServer container)
          {
            log.info("Server Disappeared[%s]", container.getName());
            listener.serverRemoved(container);
          }

          @Override
          public DruidServer updateContainer(DruidServer oldContainer, DruidServer newContainer)
          {
            log.info("Server updated[%s]", oldContainer.getName());
            return listener.serverUpdated(oldContainer, newContainer);
          }

          @Override
          public Object deserializeInventory(byte[] bytes)
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer addInventory(
              final DruidServer container,
              String inventoryKey,
              final Object inventory
          )
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer updateInventory(
              DruidServer container, String inventoryKey, Object inventory
          )
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer removeInventory(final DruidServer container, String inventoryKey)
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public void inventoryInitialized()
          {
            log.info("Server inventory initialized.");
            listener.initialized();
          }
        }
    );
  }

  public void registerListener(Listener listener)
  {
    Preconditions.checkArgument(this.listener == null, "listener registered already.");
    this.listener = listener;
  }

  public interface Listener
  {
    void serverAdded(DruidServer server);
    DruidServer serverUpdated(DruidServer oldServer, DruidServer newServer);
    void serverRemoved(DruidServer server);
    void initialized();
  }
}
