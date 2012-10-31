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

package com.metamx.druid.merger.worker;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import java.util.Arrays;

/**
 * The CuratorCoordinator provides methods to use Curator. Persistent ZK paths are created on {@link #start()}.
 */
public class WorkerCuratorCoordinator
{
  private static final Logger log = new Logger(WorkerCuratorCoordinator.class);
  private static final Joiner JOINER = Joiner.on("/");

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final CuratorFramework curatorFramework;
  private final Worker worker;

  private final String baseAnnouncementsPath;
  private final String baseTaskPath;
  private final String baseStatusPath;

  private volatile boolean started;

  public WorkerCuratorCoordinator(
      ObjectMapper jsonMapper,
      IndexerZkConfig config,
      CuratorFramework curatorFramework,
      Worker worker
  )
  {
    this.jsonMapper = jsonMapper;
    this.curatorFramework = curatorFramework;
    this.worker = worker;

    this.baseAnnouncementsPath = getPath(Arrays.asList(config.getAnnouncementPath(), worker.getHost()));
    this.baseTaskPath = getPath(Arrays.asList(config.getTaskPath(), worker.getHost()));
    this.baseStatusPath = getPath(Arrays.asList(config.getStatusPath(), worker.getHost()));
  }

  @LifecycleStart
  public void start() throws Exception
  {
    log.info("Starting WorkerCuratorCoordinator for server[%s]", worker.getHost());
    synchronized (lock) {
      if (started) {
        return;
      }

      makePathIfNotExisting(
          getTaskPathForWorker(),
          CreateMode.PERSISTENT,
          ImmutableMap.of("created", new DateTime().toString())
      );
      makePathIfNotExisting(
          getStatusPathForWorker(),
          CreateMode.PERSISTENT,
          ImmutableMap.of("created", new DateTime().toString())
      );
      makePathIfNotExisting(
          getAnnouncementsPathForWorker(),
          CreateMode.EPHEMERAL,
          worker
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop() throws Exception
  {
    log.info("Stopping WorkerCuratorCoordinator for worker[%s]", worker.getHost());
    synchronized (lock) {
      if (!started) {
        return;
      }

      curatorFramework.delete()
                      .guaranteed()
                      .forPath(getAnnouncementsPathForWorker());

      started = false;
    }
  }

  public void makePathIfNotExisting(String path, CreateMode mode, Object data) throws Exception
  {
    if (curatorFramework.checkExists().forPath(path) == null) {
      try {
        curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(mode)
                        .forPath(path, jsonMapper.writeValueAsBytes(data));
      }
      catch (Exception e) {
        log.warn(e, "Could not create path[%s], perhaps it already exists?", path);
      }
    }
  }

  public String getPath(Iterable<String> parts)
  {
    return JOINER.join(parts);
  }

  public String getAnnouncementsPathForWorker()
  {
    return baseAnnouncementsPath;
  }

  public String getTaskPathForWorker()
  {
    return baseTaskPath;
  }

  public String getTaskPathForId(String taskId)
  {
    return getPath(Arrays.asList(baseTaskPath, taskId));
  }

  public String getStatusPathForWorker()
  {
    return baseStatusPath;
  }

  public String getStatusPathForId(String statusId)
  {
    return getPath(Arrays.asList(baseStatusPath, statusId));
  }

  public boolean statusExists(String id)
  {
    try {
      return (curatorFramework.checkExists().forPath(getStatusPathForId(id)) != null);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void unannounceTask(String taskId)
  {
    try {
      curatorFramework.delete().guaranteed().forPath(getTaskPathForId(taskId));
    }
    catch (Exception e) {
      log.warn("Could not delete task path for task[%s], looks like it already went away", taskId);
    }
  }

  public void announceStatus(TaskStatus status)
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(
                            getStatusPathForId(status.getId()),
                            jsonMapper.writeValueAsBytes(status)
                        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public void updateStatus(TaskStatus status)
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        if (curatorFramework.checkExists().forPath(getStatusPathForId(status.getId())) == null) {
          announceStatus(status);
          return;
        }

        curatorFramework.setData()
                        .forPath(
                            getStatusPathForId(status.getId()),
                            jsonMapper.writeValueAsBytes(status)
                        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}