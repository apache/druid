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

package io.druid.curator.inventory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An InventoryManager watches updates to inventory on Zookeeper (or some other discovery-like service publishing
 * system).  It is built up on two object types: containers and inventory objects.
 * <p/>
 * The logic of the InventoryManager just maintains a local cache of the containers and inventory it sees on ZK.  It
 * provides methods for getting at the container objects, which house the actual individual pieces of inventory.
 * <p/>
 * A Strategy is provided to the constructor of an Inventory manager, this strategy provides all of the
 * object-specific logic to serialize, deserialize, compose and alter the container and inventory objects.
 */
public class CuratorInventoryManager<ContainerClass, InventoryClass>
{
  private static final Logger log = new Logger(CuratorInventoryManager.class);

  private final Object lock = new Object();

  private final CuratorFramework curatorFramework;
  private final InventoryManagerConfig config;
  private final CuratorInventoryManagerStrategy<ContainerClass, InventoryClass> strategy;

  private final ConcurrentMap<String, ContainerHolder> containers;
  private final Set<ContainerHolder> uninitializedInventory;
  private final PathChildrenCacheFactory cacheFactory;
  private final ExecutorService pathChildrenCacheExecutor;

  private volatile PathChildrenCache childrenCache;

  public CuratorInventoryManager(
      CuratorFramework curatorFramework,
      InventoryManagerConfig config,
      ExecutorService exec,
      CuratorInventoryManagerStrategy<ContainerClass, InventoryClass> strategy
  )
  {
    this.curatorFramework = curatorFramework;
    this.config = config;
    this.strategy = strategy;

    this.containers = new MapMaker().makeMap();
    this.uninitializedInventory = Sets.newConcurrentHashSet();

    this.pathChildrenCacheExecutor = exec;
    this.cacheFactory = new PathChildrenCacheFactory.Builder()
        //NOTE: cacheData is temporarily set to false and we get data directly from ZK on each event.
        //this is a workaround to solve curator's out-of-order events problem
        //https://issues.apache.org/jira/browse/CURATOR-191
        .withCacheData(false)
        .withCompressed(true)
        .withExecutorService(pathChildrenCacheExecutor)
        .withShutdownExecutorOnClose(false)
        .build();
  }

  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (lock) {
      if (childrenCache != null) {
        return;
      }

      childrenCache = cacheFactory.make(curatorFramework, config.getContainerPath());
    }

    childrenCache.getListenable().addListener(new ContainerCacheListener());

    try {
      childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }
    catch (Exception e) {
      synchronized (lock) {
        try {
          stop();
        }
        catch (IOException e1) {
          log.error(e1, "Exception when stopping InventoryManager that couldn't start.");
        }
      }
      throw e;
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (lock) {
      if (childrenCache == null) {
        return;
      }

      // This close() call actually calls shutdownNow() on the executor registered with the Cache object...
      childrenCache.close();
      childrenCache = null;
    }

    Closer closer = Closer.create();
    for (ContainerHolder containerHolder : containers.values()) {
      closer.register(containerHolder.getCache());
    }
    try {
      closer.close();
    }
    finally {
      pathChildrenCacheExecutor.shutdown();
    }
  }

  public InventoryManagerConfig getConfig()
  {
    return config;
  }

  public ContainerClass getInventoryValue(String containerKey)
  {
    final ContainerHolder containerHolder = containers.get(containerKey);
    return containerHolder == null ? null : containerHolder.getContainer();
  }

  public Iterable<ContainerClass> getInventory()
  {
    return Iterables.transform(
        containers.values(),
        new Function<ContainerHolder, ContainerClass>()
        {
          @Override
          public ContainerClass apply(ContainerHolder input)
          {
            return input.getContainer();
          }
        }
    );
  }

  private byte[] getZkDataForNode(String path)
  {
    try {
      return curatorFramework.getData().decompressed().forPath(path);
    }
    catch (Exception ex) {
      log.warn(ex, "Exception while getting data for node %s", path);
      return null;
    }
  }

  private class ContainerHolder
  {
    private final AtomicReference<ContainerClass> container;
    private final PathChildrenCache cache;
    private boolean initialized = false;

    ContainerHolder(
        ContainerClass container,
        PathChildrenCache cache
    )
    {
      this.container = new AtomicReference<ContainerClass>(container);
      this.cache = cache;
    }

    private ContainerClass getContainer()
    {
      return container.get();
    }

    private void setContainer(ContainerClass newContainer)
    {
      container.set(newContainer);
    }

    private PathChildrenCache getCache()
    {
      return cache;
    }
  }

  private class ContainerCacheListener implements PathChildrenCacheListener
  {
    private volatile boolean containersInitialized = false;
    private volatile boolean doneInitializing = false;

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
      switch (event.getType()) {
        case CHILD_ADDED:
          synchronized (lock) {
            final ChildData child = event.getData();

            byte[] data = getZkDataForNode(child.getPath());
            if(data == null) {
              log.info("Ignoring event: Type - %s , Path - %s , Version - %s",
                  event.getType(),
                  child.getPath(),
                  child.getStat().getVersion());
              return;
            }

            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerClass container = strategy.deserializeContainer(data);

            // This would normally be a race condition, but the only thing that should be mutating the containers
            // map is this listener, which should never run concurrently.  If the same container is going to disappear
            // and come back, we expect a removed event in between.
            if (containers.containsKey(containerKey)) {
              log.error("New node[%s] but there was already one.  That's not good, ignoring new one.", child.getPath());
            } else {
              final String inventoryPath = StringUtils.format("%s/%s", config.getInventoryPath(), containerKey);
              PathChildrenCache inventoryCache = cacheFactory.make(curatorFramework, inventoryPath);
              inventoryCache.getListenable().addListener(new InventoryCacheListener(containerKey, inventoryPath));

              containers.put(containerKey, new ContainerHolder(container, inventoryCache));

              log.debug("Starting inventory cache for %s, inventoryPath %s", containerKey, inventoryPath);
              inventoryCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
              strategy.newContainer(container);
            }
          }
          break;
        case CHILD_REMOVED:
          synchronized (lock) {
            final ChildData child = event.getData();
            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerHolder removed = containers.remove(containerKey);
            if (removed == null) {
              log.warn("Container[%s] removed that wasn't a container!?", child.getPath());
              break;
            }

            // This close() call actually calls shutdownNow() on the executor registered with the Cache object, it
            // better have its own executor or ignore shutdownNow() calls...
            log.debug("Closing inventory cache for %s. Also removing listeners.", containerKey);
            removed.getCache().getListenable().clear();
            removed.getCache().close();
            strategy.deadContainer(removed.getContainer());

            // also remove node from uninitilized, in case a nodes gets removed while we are starting up
            synchronized (removed) {
              markInventoryInitialized(removed);
            }
          }
          break;
        case CHILD_UPDATED:
          synchronized (lock) {
            final ChildData child = event.getData();

            byte[] data = getZkDataForNode(child.getPath());
            if (data == null) {
              log.info("Ignoring event: Type - %s , Path - %s , Version - %s",
                  event.getType(),
                  child.getPath(),
                  child.getStat().getVersion());
              return;
            }

            final String containerKey = ZKPaths.getNodeFromPath(child.getPath());

            final ContainerClass container = strategy.deserializeContainer(data);

            log.debug("Container[%s] updated.", child.getPath());
            ContainerHolder holder = containers.get(containerKey);
            if (holder == null) {
              log.warn("Container update[%s], but the old container didn't exist!?  Ignoring.", child.getPath());
            } else {
              synchronized (holder) {
                holder.setContainer(strategy.updateContainer(holder.getContainer(), container));
              }
            }
          }
          break;
        case INITIALIZED:
          synchronized (lock) {
            // must await initialized of all containerholders
            for (ContainerHolder holder : containers.values()) {
              synchronized (holder) {
                if (!holder.initialized) {
                  uninitializedInventory.add(holder);
                }
              }
            }
            containersInitialized = true;
            maybeDoneInitializing();
          }
          break;
        case CONNECTION_SUSPENDED:
        case CONNECTION_RECONNECTED:
        case CONNECTION_LOST:
          // do nothing
      }
    }

    // must be run in synchronized(lock) { synchronized(holder) { ... } } block
    private void markInventoryInitialized(final ContainerHolder holder)
    {
      holder.initialized = true;
      uninitializedInventory.remove(holder);
      maybeDoneInitializing();
    }

    private void maybeDoneInitializing()
    {
      if (doneInitializing) {
        return;
      }

      // only fire if we are done initializing the parent PathChildrenCache
      if (containersInitialized && uninitializedInventory.isEmpty()) {
        doneInitializing = true;
        strategy.inventoryInitialized();
      }
    }

    private class InventoryCacheListener implements PathChildrenCacheListener
    {
      private final String containerKey;
      private final String inventoryPath;

      public InventoryCacheListener(String containerKey, String inventoryPath)
      {
        this.containerKey = containerKey;
        this.inventoryPath = inventoryPath;

        log.info("Created new InventoryCacheListener for %s", inventoryPath);
      }

      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
      {
        final ContainerHolder holder = containers.get(containerKey);
        if (holder == null) {
          return;
        }

        switch (event.getType()) {
          case CHILD_ADDED: {
            final ChildData child = event.getData();

            byte[] data = getZkDataForNode(child.getPath());
            if (data == null) {
              log.info("Ignoring event: Type - %s , Path - %s , Version - %s",
                  event.getType(),
                  child.getPath(),
                  child.getStat().getVersion());
              return;
            }

            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());
            log.debug("CHILD_ADDED[%s] with version[%s]", child.getPath(), event.getData().getStat().getVersion());

            final InventoryClass addedInventory = strategy.deserializeInventory(data);

            synchronized (holder) {
              holder.setContainer(strategy.addInventory(holder.getContainer(), inventoryKey, addedInventory));
            }
            break;
          }

          case CHILD_UPDATED: {
            final ChildData child = event.getData();

            byte[] data = getZkDataForNode(child.getPath());
            if (data == null) {
              log.info("Ignoring event: Type - %s , Path - %s , Version - %s",
                  event.getType(),
                  child.getPath(),
                  child.getStat().getVersion());
              return;
            }

            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());
            log.debug("CHILD_UPDATED[%s] with version[%s]", child.getPath(), event.getData().getStat().getVersion());

            final InventoryClass updatedInventory = strategy.deserializeInventory(data);

            synchronized (holder) {
              holder.setContainer(strategy.updateInventory(holder.getContainer(), inventoryKey, updatedInventory));
            }

            break;
          }

          case CHILD_REMOVED: {
            final ChildData child = event.getData();
            final String inventoryKey = ZKPaths.getNodeFromPath(child.getPath());
            log.debug("CHILD_REMOVED[%s] with version[%s]", child.getPath(), event.getData().getStat().getVersion());

            synchronized (holder) {
              holder.setContainer(strategy.removeInventory(holder.getContainer(), inventoryKey));
            }

            break;
          }
          case INITIALIZED: {
            // make sure to acquire locks in (lock -> holder) order
            synchronized (lock) {
              synchronized (holder) {
                markInventoryInitialized(holder);
              }
            }

            break;
          }
          case CONNECTION_SUSPENDED:
          case CONNECTION_RECONNECTED:
          case CONNECTION_LOST:
            // do nothing
        }
      }
    }
  }
}
