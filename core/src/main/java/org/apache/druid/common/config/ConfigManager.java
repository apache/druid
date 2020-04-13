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

package org.apache.druid.common.config;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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

  private volatile PollingCallable poller;

  @Inject
  public ConfigManager(MetadataStorageConnector dbConnector, Supplier<MetadataStorageTablesConfig> dbTables, Supplier<ConfigManagerConfig> config)
  {
    this.dbConnector = dbConnector;
    this.config = config;

    this.exec = ScheduledExecutors.fixed(1, "config-manager-%s");
    this.watchedConfigs = new ConcurrentHashMap<>();

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
          exec,
          new Duration(0),
          config.get().getPollDuration().toStandardDuration(),
          poller
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
        log.debug("Creating watch for key[%s]", key);

        holder = exec.submit(
            new Callable<ConfigHolder<T>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public ConfigHolder<T> call()
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
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    return holder.getReference();
  }


  public <T> SetResult set(final String key, final ConfigSerde<T> serde, final T obj)
  {
    if (obj == null || !started) {
      if (obj == null) {
        return SetResult.fail(new IllegalAccessException("input obj is null"));
      } else {
        return SetResult.fail(new IllegalStateException("configManager is not started yet"));
      }
    }

    final byte[] newBytes = serde.serialize(obj);

    try {
      exec.submit(
          () -> {
            dbConnector.insertOrUpdate(configTable, "name", "payload", key, newBytes);

            final ConfigHolder configHolder = watchedConfigs.get(key);
            if (configHolder != null) {
              configHolder.swapIfNew(newBytes);
            }

            return true;
          }
      ).get();
      return SetResult.ok();
    }
    catch (Exception e) {
      log.warn(e, "Failed to set[%s]", key);
      return SetResult.fail(e);
    }
  }

  public static class SetResult
  {
    private final Exception exception;

    public static SetResult ok()
    {
      return new SetResult(null);
    }

    public static SetResult fail(Exception e)
    {
      return new SetResult(e);
    }

    private SetResult(@Nullable Exception exception)
    {
      this.exception = exception;
    }

    public boolean isOk()
    {
      return exception == null;
    }

    public Exception getException()
    {
      return exception;
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
    public ScheduledExecutors.Signal call()
    {
      if (stop) {
        return ScheduledExecutors.Signal.STOP;
      }

      poll();
      return ScheduledExecutors.Signal.REPEAT;
    }
  }
}
