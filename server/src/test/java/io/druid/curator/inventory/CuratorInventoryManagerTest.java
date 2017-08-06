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

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 */
public class CuratorInventoryManagerTest extends CuratorTestBase
{
  private ExecutorService exec;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    exec = Execs.singleThreaded("curator-inventory-manager-test-%s");
  }

  @After
  public void tearDown() throws Exception
  {
    tearDownServerAndCurator();
  }

  @Test
  public void testSanity() throws Exception
  {
    final MapStrategy strategy = new MapStrategy();
    CuratorInventoryManager<Map<String, Integer>, Integer> manager = new CuratorInventoryManager<Map<String, Integer>, Integer>(
        curator, new StringInventoryManagerConfig("/container", "/inventory"), exec, strategy
    );

    curator.start();
    curator.blockUntilConnected();

    manager.start();

    Assert.assertTrue(Iterables.isEmpty(manager.getInventory()));

    CountDownLatch containerLatch = new CountDownLatch(1);
    strategy.setNewContainerLatch(containerLatch);
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/container/billy", new byte[]{});

    Assert.assertTrue(timing.awaitLatch(containerLatch));
    strategy.setNewContainerLatch(null);

    final Iterable<Map<String, Integer>> inventory = manager.getInventory();
    Assert.assertTrue(Iterables.getOnlyElement(inventory).isEmpty());

    CountDownLatch inventoryLatch = new CountDownLatch(2);
    strategy.setNewInventoryLatch(inventoryLatch);
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1", Ints.toByteArray(100));
    curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/bob", Ints.toByteArray(2287));

    Assert.assertTrue(timing.awaitLatch(inventoryLatch));
    strategy.setNewInventoryLatch(null);

    verifyInventory(manager);

    CountDownLatch deleteLatch = new CountDownLatch(1);
    strategy.setDeadInventoryLatch(deleteLatch);
    curator.delete().forPath("/inventory/billy/1");

    Assert.assertTrue(timing.awaitLatch(deleteLatch));
    strategy.setDeadInventoryLatch(null);

    Assert.assertEquals(1, manager.getInventoryValue("billy").size());
    Assert.assertEquals(2287, manager.getInventoryValue("billy").get("bob").intValue());

    inventoryLatch = new CountDownLatch(1);
    strategy.setNewInventoryLatch(inventoryLatch);
    curator.create().withMode(CreateMode.EPHEMERAL).forPath("/inventory/billy/1", Ints.toByteArray(100));

    Assert.assertTrue(timing.awaitLatch(inventoryLatch));
    strategy.setNewInventoryLatch(null);

    verifyInventory(manager);

    final CountDownLatch latch = new CountDownLatch(1);
    curator.getCuratorListenable().addListener(
        new CuratorListener() {
          @Override
          public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception
          {
            if (event.getType() == CuratorEventType.WATCHED
                && event.getWatchedEvent().getState() == Watcher.Event.KeeperState.Disconnected) {
              latch.countDown();
            }
          }
        }
    );

    server.stop();
    Assert.assertTrue(timing.awaitLatch(latch));

    verifyInventory(manager);

    Thread.sleep(50); // Wait a bit

    verifyInventory(manager);
  }

  private void verifyInventory(CuratorInventoryManager<Map<String, Integer>, Integer> manager)
  {
    final Map<String, Integer> vals = manager.getInventoryValue("billy");
    Assert.assertEquals(2, vals.size());
    Assert.assertEquals(100, vals.get("1").intValue());
    Assert.assertEquals(2287, vals.get("bob").intValue());
  }

  private static class StringInventoryManagerConfig implements InventoryManagerConfig
  {
    private final String containerPath;
    private final String inventoryPath;

    private StringInventoryManagerConfig(
        String containerPath,
        String inventoryPath
    )
    {
      this.containerPath = containerPath;
      this.inventoryPath = inventoryPath;
    }

    @Override
    public String getContainerPath()
    {
      return containerPath;
    }

    @Override
    public String getInventoryPath()
    {
      return inventoryPath;
    }
  }

  private static class MapStrategy implements CuratorInventoryManagerStrategy<Map<String, Integer>, Integer>
  {
    private volatile CountDownLatch newContainerLatch = null;
    private volatile CountDownLatch deadContainerLatch = null;
    private volatile CountDownLatch newInventoryLatch = null;
    private volatile CountDownLatch deadInventoryLatch = null;
    private volatile boolean initialized = false;

    @Override
    public Map<String, Integer> deserializeContainer(byte[] bytes)
    {
      return Maps.newTreeMap();
    }

    @Override
    public Integer deserializeInventory(byte[] bytes)
    {
      return Ints.fromByteArray(bytes);
    }

    @Override
    public void newContainer(Map<String, Integer> newContainer)
    {
      if (newContainerLatch != null) {
        newContainerLatch.countDown();
      }
    }

    @Override
    public void deadContainer(Map<String, Integer> deadContainer)
    {
      if (deadContainerLatch != null) {
        deadContainerLatch.countDown();
      }
    }

    @Override
    public Map<String, Integer> updateContainer(Map<String, Integer> oldContainer, Map<String, Integer> newContainer)
    {
      newContainer.putAll(oldContainer);
      return newContainer;
    }

    @Override
    public Map<String, Integer> addInventory(Map<String, Integer> container, String inventoryKey, Integer inventory)
    {
      container.put(inventoryKey, inventory);
      if (newInventoryLatch != null) {
        newInventoryLatch.countDown();
      }
      return container;
    }

    @Override
    public Map<String, Integer> updateInventory(
        Map<String, Integer> container, String inventoryKey, Integer inventory
    )
    {
      return addInventory(container, inventoryKey, inventory);
    }

    @Override
    public Map<String, Integer> removeInventory(Map<String, Integer> container, String inventoryKey)
    {
      container.remove(inventoryKey);
      if (deadInventoryLatch != null) {
        deadInventoryLatch.countDown();
      }
      return container;
    }

    private void setNewContainerLatch(CountDownLatch newContainerLatch)
    {
      this.newContainerLatch = newContainerLatch;
    }

    private void setDeadContainerLatch(CountDownLatch deadContainerLatch)
    {
      this.deadContainerLatch = deadContainerLatch;
    }

    private void setNewInventoryLatch(CountDownLatch newInventoryLatch)
    {
      this.newInventoryLatch = newInventoryLatch;
    }

    private void setDeadInventoryLatch(CountDownLatch deadInventoryLatch)
    {
      this.deadInventoryLatch = deadInventoryLatch;
    }

    @Override
    public void inventoryInitialized()
    {
      initialized = true;
    }
  }
}
