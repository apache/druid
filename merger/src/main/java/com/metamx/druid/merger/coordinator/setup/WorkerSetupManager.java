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

package com.metamx.druid.merger.coordinator.setup;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.coordinator.config.WorkerSetupManagerConfig;
import org.apache.commons.collections.MapUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class WorkerSetupManager
{
  private static final Logger log = new Logger(WorkerSetupManager.class);

  private final DBI dbi;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService exec;
  private final WorkerSetupManagerConfig config;

  private final Object lock = new Object();

  private volatile AtomicReference<WorkerSetupData> workerSetupData = new AtomicReference<WorkerSetupData>(null);
  private volatile boolean started = false;

  public WorkerSetupManager(
      DBI dbi,
      ScheduledExecutorService exec,
      ObjectMapper jsonMapper,
      WorkerSetupManagerConfig config
  )
  {
    this.dbi = dbi;
    this.exec = exec;
    this.jsonMapper = jsonMapper;
    this.config = config;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          config.getPollDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              poll();
            }
          }
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

      started = false;
    }
  }

  public void poll()
  {
    try {
      List<WorkerSetupData> setupDataList = dbi.withHandle(
          new HandleCallback<List<WorkerSetupData>>()
          {
            @Override
            public List<WorkerSetupData> withHandle(Handle handle) throws Exception
            {
              return handle.createQuery(
                  String.format(
                      "SELECT payload FROM %s WHERE name = :name",
                      config.getConfigTable()
                  )
              )
                           .bind("name", config.getWorkerSetupConfigName())
                           .fold(
                               Lists.<WorkerSetupData>newArrayList(),
                               new Folder3<ArrayList<WorkerSetupData>, Map<String, Object>>()
                               {
                                 @Override
                                 public ArrayList<WorkerSetupData> fold(
                                     ArrayList<WorkerSetupData> workerNodeConfigurations,
                                     Map<String, Object> stringObjectMap,
                                     FoldController foldController,
                                     StatementContext statementContext
                                 ) throws SQLException
                                 {
                                   try {
                                     // stringObjectMap lowercases and jackson may fail serde
                                     workerNodeConfigurations.add(
                                         jsonMapper.readValue(
                                             MapUtils.getString(stringObjectMap, "payload"),
                                             WorkerSetupData.class
                                         )
                                     );
                                     return workerNodeConfigurations;
                                   }
                                   catch (Exception e) {
                                     throw Throwables.propagate(e);
                                   }
                                 }
                               }
                           );
            }
          }
      );

      if (setupDataList.isEmpty()) {
        throw new ISE("WTF?! No configuration found for worker nodes!");
      } else if (setupDataList.size() != 1) {
        throw new ISE("WTF?! Found more than one configuration for worker nodes");
      }

      workerSetupData.set(setupDataList.get(0));
    }
    catch (Exception e) {
      log.error(e, "Exception while polling for worker setup data!");
    }
  }

  @SuppressWarnings("unchecked")
  public WorkerSetupData getWorkerSetupData()
  {
    synchronized (lock) {
      if (!started) {
        throw new ISE("Must start WorkerSetupManager first!");
      }

      return workerSetupData.get();
    }
  }

  public boolean setWorkerSetupData(final WorkerSetupData value)
  {
    synchronized (lock) {
      try {
        if (!started) {
          throw new ISE("Must start WorkerSetupManager first!");
        }

        dbi.withHandle(
            new HandleCallback<Void>()
            {
              @Override
              public Void withHandle(Handle handle) throws Exception
              {
                handle.createStatement(
                    String.format(
                        "INSERT INTO %s (name, payload) VALUES (:name, :payload) ON DUPLICATE KEY UPDATE payload = :payload",
                        config.getConfigTable()
                    )
                )
                      .bind("name", config.getWorkerSetupConfigName())
                      .bind("payload", jsonMapper.writeValueAsString(value))
                      .execute();

                return null;
              }
            }
        );

        workerSetupData.set(value);
      }
      catch (Exception e) {
        log.error(e, "Exception updating worker config");
        return false;
      }
    }

    return true;
  }
}
