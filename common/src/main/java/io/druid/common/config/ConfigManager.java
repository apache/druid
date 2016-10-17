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

package io.druid.common.config;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;

import org.joda.time.Duration;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class ConfigManager
{
  private static final Logger log = new Logger(ConfigManager.class);

  private final Object lock = new Object();
  private boolean started = false;

  private final MetadataStorageConnector dbConnector;
  private final Supplier<ConfigManagerConfig> config;

  private final ScheduledExecutorService exec;
  private final ConcurrentMap<String, ConfigHolder> watchedConfigs;
  private final String configTable;

  private volatile ConfigManager.PollingCallable poller;

  @Inject
  public ConfigManager(MetadataStorageConnector dbConnector, Supplier<MetadataStorageTablesConfig> dbTables, Supplier<ConfigManagerConfig> config)
  {
    this.dbConnector = dbConnector;
    this.config = config;

    this.exec = ScheduledExecutors.fixed(1, "config-manager-%s");
    this.watchedConfigs = Maps.newConcurrentMap();

    this.configTable = dbTables.get().getConfigTable();
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      poller = new PollingCallable();
      ScheduledExecutors.scheduleWithFixedDelay(
          exec, new Duration(0), config.get().getPollDuration().toStandardDuration(), poller
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      poller.stop();
      poller = null;

      started = false;
    }
  }

  private void poll()
  {
    for (Map.Entry<String, ConfigHolder> entry : watchedConfigs.entrySet()) {
      try {
        if (entry.getValue().swapIfNew(dbConnector.lookup(configTable, "name", "payload", entry.getKey()))) {
          log.info("New value for key[%s] seen.", entry.getKey());
        }
      }
      catch (Exception e) {
        log.warn(e, "Exception when checking property[%s]", entry.getKey());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T> AtomicReference<T> watchConfig(final String key, final ConfigSerde<T> serde)
  {
    ConfigHolder<T> holder = watchedConfigs.get(key);
    if (holder == null) {
      try {
        log.info("Creating watch for key[%s]", key);

        holder = exec.submit(
            new Callable<ConfigHolder<T>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public ConfigHolder<T> call() throws Exception
              {
                if (!started) {
                  watchedConfigs.put(key, new ConfigHolder<T>(null, serde));
                } else {
                  try {
                    // Multiple of these callables can be submitted at the same time, but the callables themselves
                    // are executed serially, so double check that it hasn't already been populated.
                    if (!watchedConfigs.containsKey(key)) {
                      byte[] value = dbConnector.lookup(configTable, "name", "payload", key);
                      ConfigHolder<T> holder = new ConfigHolder<T>(value, serde);
                      watchedConfigs.put(key, holder);
                    }
                  }
                  catch (Exception e) {
                    log.warn(e, "Failed loading config for key[%s]", key);
                    watchedConfigs.put(key, new ConfigHolder<T>(null, serde));
                  }
                }

                return watchedConfigs.get(key);
              }
            }
        ).get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
      catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
    }

    return holder.getReference();
  }


  public <T> boolean set(final String key, final ConfigSerde<T> serde, final T obj)
  {
    if (obj == null || !started) {
      return false;
    }

    final byte[] newBytes = serde.serialize(obj);

    try {
      return exec.submit(
          new Callable<Boolean>()
          {
            @Override
            public Boolean call() throws Exception
            {
              dbConnector.insertOrUpdate(configTable, "name", "payload", key, newBytes);

              final ConfigHolder configHolder = watchedConfigs.get(key);
              if (configHolder != null) {
                configHolder.swapIfNew(newBytes);
              }

              return true;
            }
          }
      ).get();
    }
    catch (Exception e) {
      log.warn(e, "Failed to set[%s]", key);
      return false;
    }
  }

  private static class ConfigHolder<T>
  {
    private final AtomicReference<byte[]> rawBytes;
    private final ConfigSerde<T> serde;
    private final AtomicReference<T> reference;

    ConfigHolder(
        byte[] rawBytes,
        ConfigSerde<T> serde
    )
    {
      this.rawBytes = new AtomicReference<byte[]>(rawBytes);
      this.serde = serde;
      this.reference = new AtomicReference<T>(serde.deserialize(rawBytes));
    }

    public AtomicReference<T> getReference()
    {
      return reference;
    }

    public boolean swapIfNew(byte[] newBytes)
    {
      if (!Arrays.equals(newBytes, rawBytes.get())) {
        reference.set(serde.deserialize(newBytes));
        rawBytes.set(newBytes);
        return true;
      }
      return false;
    }
  }

  private class PollingCallable implements Callable<ScheduledExecutors.Signal>
  {
    private volatile boolean stop = false;

    void stop()
    {
      stop = true;
    }

    @Override
    public ScheduledExecutors.Signal call() throws Exception
    {
      if (stop) {
        return ScheduledExecutors.Signal.STOP;
      }

      poll();
      return ScheduledExecutors.Signal.REPEAT;
    }
  }
}
