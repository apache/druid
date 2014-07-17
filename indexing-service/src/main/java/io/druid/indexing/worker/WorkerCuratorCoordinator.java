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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
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
  private final RemoteTaskRunnerConfig config;
  private final CuratorFramework curatorFramework;
  private final Announcer announcer;

  private final String baseAnnouncementsPath;
  private final String baseTaskPath;
  private final String baseStatusPath;

  private volatile Worker worker;
  private volatile boolean started;

  @Inject
  public WorkerCuratorCoordinator(
      ObjectMapper jsonMapper,
      ZkPathsConfig zkPaths,
      RemoteTaskRunnerConfig config,
      CuratorFramework curatorFramework,
      Worker worker
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.curatorFramework = curatorFramework;
    this.worker = worker;

    this.announcer = new Announcer(curatorFramework, MoreExecutors.sameThreadExecutor());

    this.baseAnnouncementsPath = getPath(Arrays.asList(zkPaths.getIndexerAnnouncementPath(), worker.getHost()));
    this.baseTaskPath = getPath(Arrays.asList(zkPaths.getIndexerTaskPath(), worker.getHost()));
    this.baseStatusPath = getPath(Arrays.asList(zkPaths.getIndexerStatusPath(), worker.getHost()));
  }

  @LifecycleStart
  public void start() throws Exception
  {
    log.info("WorkerCuratorCoordinator good to go sir. Server[%s]", worker.getHost());
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
      announcer.start();
      announcer.announce(getAnnouncementsPathForWorker(), jsonMapper.writeValueAsBytes(worker));

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

      announcer.unannounce(getAnnouncementsPathForWorker());
      announcer.stop();

      started = false;
    }
  }

  public void makePathIfNotExisting(String path, CreateMode mode, Object data) throws Exception
  {
    if (curatorFramework.checkExists().forPath(path) == null) {
      try {
        byte[] rawBytes = jsonMapper.writeValueAsBytes(data);
        if (rawBytes.length > config.getMaxZnodeBytes()) {
          throw new ISE(
              "Length of raw bytes for task too large[%,d > %,d]",
              rawBytes.length,
              config.getMaxZnodeBytes()
          );
        }

        curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(mode)
                        .forPath(path, rawBytes);
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

  public Worker getWorker()
  {
    return worker;
  }

  public void unannounceTask(String taskId)
  {
    try {
      curatorFramework.delete().guaranteed().forPath(getTaskPathForId(taskId));
    }
    catch (Exception e) {
      log.warn(e, "Could not delete task path for task[%s]", taskId);
    }
  }

  public void announceTastAnnouncement(TaskAnnouncement announcement)
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        byte[] rawBytes = jsonMapper.writeValueAsBytes(announcement);
        if (rawBytes.length > config.getMaxZnodeBytes()) {
          throw new ISE(
              "Length of raw bytes for task too large[%,d > %,d]", rawBytes.length, config.getMaxZnodeBytes()
          );
        }

        curatorFramework.create()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(
                            getStatusPathForId(announcement.getTaskStatus().getId()), rawBytes
                        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public void updateAnnouncement(TaskAnnouncement announcement)
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      try {
        if (curatorFramework.checkExists().forPath(getStatusPathForId(announcement.getTaskStatus().getId())) == null) {
          announceTastAnnouncement(announcement);
          return;
        }
        byte[] rawBytes = jsonMapper.writeValueAsBytes(announcement);
        if (rawBytes.length > config.getMaxZnodeBytes()) {
          throw new ISE(
              "Length of raw bytes for task too large[%,d > %,d]", rawBytes.length, config.getMaxZnodeBytes()
          );
        }

        curatorFramework.setData()
                        .forPath(
                            getStatusPathForId(announcement.getTaskStatus().getId()), rawBytes
                        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public void updateWorkerAnnouncement(Worker newWorker) throws Exception
  {
    synchronized (lock) {
      if (!started) {
        throw new ISE("Cannot update worker! Not Started!");
      }

      this.worker = newWorker;
      announcer.update(getAnnouncementsPathForWorker(), jsonMapper.writeValueAsBytes(newWorker));
    }
  }
}
