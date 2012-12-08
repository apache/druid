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

import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public abstract class InventoryManager<T>
{
  private final Object lock = new Object();
  private volatile boolean started = false;

  private final MasterPeon masterPeon;
  private final ConcurrentHashMap<String, PhoneBookPeon<?>> dataSourcePeons;
  private final ConcurrentHashMap<String, T> dataSources;

  private final Logger log;
  private final PhoneBook yp;
  private final InventoryManagerConfig config;

  private volatile InventoryManagementStrategy<T> strategy = null;

  public InventoryManager(
      Logger log,
      InventoryManagerConfig config,
      PhoneBook zkPhoneBook
  )
  {
    this.log = log;
    this.config = config;
    this.yp = zkPhoneBook;

    this.masterPeon = new MasterPeon();
    this.dataSourcePeons = new ConcurrentHashMap<String, PhoneBookPeon<?>>();
    this.dataSources = new ConcurrentHashMap<String, T>();
  }

  public InventoryManager(
      Logger log,
      InventoryManagerConfig config,
      PhoneBook zkPhoneBook,
      InventoryManagementStrategy<T> strategy
  )
  {
    this(log, config, zkPhoneBook);
    setStrategy(strategy);
  }

  public void setStrategy(InventoryManagementStrategy<T> strategy)
  {
    if (this.strategy != null) {
      throw new ISE("Management can only handle a single strategy, you cannot change your strategy.");
    }
    this.strategy = strategy;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      if (strategy == null) {
        throw new ISE("Management requires a strategy, please provide a strategy.");
      }

      if (!yp.isStarted()) {
        throw new ISE("Management does not work without a running yellow pages.");
      }

      yp.registerListener(config.getInventoryIdPath(), masterPeon);
      doStart();

      started = true;
    }
  }

  protected void doStart() {};

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      yp.unregisterListener(config.getInventoryIdPath(), masterPeon);
      for (Map.Entry<String, PhoneBookPeon<?>> entry : dataSourcePeons.entrySet()) {
        yp.unregisterListener(
            yp.combineParts(Arrays.asList(config.getInventoryPath(), entry.getKey())), entry.getValue()
        );
      }

      dataSources.clear();
      dataSourcePeons.clear();
      doStop();

      started = false;
    }
  }

  protected void doStop() {};

  public boolean isStarted()
  {
    return started;
  }

  public T getInventoryValue(String key)
  {
    return dataSources.get(key);
  }

  public Iterable<T> getInventory()
  {
    return dataSources.values();
  }

  public void remove(List<String> nodePath)
  {
    yp.unpost(config.getInventoryIdPath(), yp.combineParts(nodePath));
  }

  private class MasterPeon implements PhoneBookPeon
  {
    @Override
    public Class getObjectClazz()
    {
      return strategy.doesSerde() ? Object.class : strategy.getContainerClass();
    }

    @Override
    public void newEntry(final String name, Object baseObject)
    {
      synchronized (lock) {
        if (!started) {
          return;
        }

        log.info("New inventory container[%s]!", name);
        if (strategy.doesSerde()) { // Hack to work around poor serialization choice
          baseObject = strategy.deserialize(name, (Map<String, String>) baseObject);
        }
        Object shouldBeNull = dataSources.put(name, strategy.getContainerClass().cast(baseObject));
        if (shouldBeNull != null) {
          log.warn(
              "Just put key[%s] into dataSources and what was there wasn't null!?  It was[%s]", name, shouldBeNull
          );
        }

        Pair<String, PhoneBookPeon<?>> pair = strategy.makeSubListener(strategy.getContainerClass().cast(baseObject));

        shouldBeNull = dataSourcePeons.put(pair.lhs, pair.rhs);
        if (shouldBeNull != null) {
          log.warn(
              "Just put key[%s] into dataSourcePeons and what was there wasn't null!?  It was[%s]", name, shouldBeNull
          );
        }

        String serviceName = yp.combineParts(Arrays.asList(config.getInventoryPath(), pair.lhs));
        log.info("Putting watch on [%s]", serviceName);
        yp.registerListener(serviceName, pair.rhs);
      }
    }

    @Override
    public void entryRemoved(String name)
    {
      synchronized (lock) {
        if (!started) {
          return;
        }

        log.info("Inventory container[%s] removed, deleting.", name);
        T removed = dataSources.remove(name);
        if (removed != null) {
          strategy.objectRemoved(removed);
        }
        else {
          log.warn("Removed empty element at path[%s]", name);
        }
        yp.unregisterListener(
            yp.combineParts(Arrays.asList(config.getInventoryPath(), name)), dataSourcePeons.remove(name)
        );
      }
    }
  }
}
