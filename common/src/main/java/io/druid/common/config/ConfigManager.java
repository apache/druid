/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.common.config;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.db.DbConnector;
import io.druid.db.DbTablesConfig;
import org.joda.time.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
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

  private final IDBI dbi;
  private final Supplier<ConfigManagerConfig> config;

  private final ScheduledExecutorService exec;
  private final ConcurrentMap<String, ConfigHolder> watchedConfigs;
  private final String selectStatement;
  private final String insertStatement;

  private volatile ConfigManager.PollingCallable poller;

  @Inject
  public ConfigManager(IDBI dbi, Supplier<DbTablesConfig> dbTables, Supplier<ConfigManagerConfig> config)
  {
    this.dbi = dbi;
    this.config = config;

    this.exec = ScheduledExecutors.fixed(1, "config-manager-%s");
    this.watchedConfigs = Maps.newConcurrentMap();

    final String configTable = dbTables.get().getConfigTable();

    this.selectStatement = String.format("SELECT payload FROM %s WHERE name = :name", configTable);
    this.insertStatement = String.format(
        DbConnector.isPostgreSQL(dbi) ?
          "BEGIN;\n" +
          "LOCK TABLE %1$s IN SHARE ROW EXCLUSIVE MODE;\n" +
          "WITH upsert AS (UPDATE %1$s SET payload=:payload WHERE name=:name RETURNING *)\n" +
          "    INSERT INTO %1$s (name, payload) SELECT :name, :payload WHERE NOT EXISTS (SELECT * FROM upsert)\n;" +
          "COMMIT;" :
          "INSERT INTO %s (name, payload) VALUES (:name, :payload) ON DUPLICATE KEY UPDATE payload = :payload",
        configTable
    );
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
        if (entry.getValue().swapIfNew(lookup(entry.getKey()))) {
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
                      byte[] value = lookup(key);
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

  public byte[] lookup(final String key)
  {
    return dbi.withHandle(
        new HandleCallback<byte[]>()
        {
          @Override
          public byte[] withHandle(Handle handle) throws Exception
          {
            List<byte[]> matched = handle.createQuery(selectStatement)
                                     .bind("name", key)
                                     .map(
                                         new ResultSetMapper<byte[]>()
                                         {
                                           @Override
                                           public byte[] map(int index, ResultSet r, StatementContext ctx)
                                               throws SQLException
                                           {
                                             return r.getBytes("payload");
                                           }
                                         }
                                     ).list();

            if (matched.isEmpty()) {
              return null;
            }

            if (matched.size() > 1) {
              throw new ISE("Error! More than one matching entry[%d] found for [%s]?!", matched.size(), key);
            }

            return matched.get(0);
          }
        }
    );
  }

  public <T> boolean set(final String key, final ConfigSerde<T> serde, final T obj)
  {
    if (obj == null) {
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
              dbi.withHandle(
                  new HandleCallback<Void>()
                  {
                    @Override
                    public Void withHandle(Handle handle) throws Exception
                    {
                      handle.createStatement(insertStatement)
                            .bind("name", key)
                            .bind("payload", newBytes)
                            .execute();
                      return null;
                    }
                  }
              );

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
