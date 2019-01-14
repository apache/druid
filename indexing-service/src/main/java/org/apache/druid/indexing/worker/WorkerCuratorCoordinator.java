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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.curator.CuratorUtils;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
      IndexerZkConfig indexerZkConfig,
      RemoteTaskRunnerConfig config,
      CuratorFramework curatorFramework,
      Worker worker
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.curatorFramework = curatorFramework;
    this.worker = worker;

    this.announcer = new Announcer(curatorFramework, Execs.directExecutor());

    this.baseAnnouncementsPath = getPath(Arrays.asList(indexerZkConfig.getAnnouncementsPath(), worker.getHost()));
    this.baseTaskPath = getPath(Arrays.asList(indexerZkConfig.getTasksPath(), worker.getHost()));
    this.baseStatusPath = getPath(Arrays.asList(indexerZkConfig.getStatusPath(), worker.getHost()));
  }

  @LifecycleStart
  public void start() throws Exception
  {
    log.info("WorkerCuratorCoordinator good to go sir. Server[%s]", worker.getHost());
    synchronized (lock) {
      if (started) {
        return;
      }

      CuratorUtils.createIfNotExists(
          curatorFramework,
          getTaskPathForWorker(),
          CreateMode.PERSISTENT,
          jsonMapper.writeValueAsBytes(ImmutableMap.of("created", DateTimes.nowUtc().toString())),
          config.getMaxZnodeBytes()
      );

      CuratorUtils.createIfNotExists(
          curatorFramework,
          getStatusPathForWorker(),
          CreateMode.PERSISTENT,
          jsonMapper.writeValueAsBytes(ImmutableMap.of("created", DateTimes.nowUtc().toString())),
          config.getMaxZnodeBytes()
      );

      announcer.start();
      announcer.announce(getAnnouncementsPathForWorker(), jsonMapper.writeValueAsBytes(worker), false);

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping WorkerCuratorCoordinator for worker[%s]", worker.getHost());
    synchronized (lock) {
      if (!started) {
        return;
      }
      announcer.stop();

      started = false;
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

  public void removeTaskRunZnode(String taskId) throws Exception
  {
    try {
      curatorFramework.delete().guaranteed().forPath(getTaskPathForId(taskId));
    }
    catch (KeeperException e) {
      log.debug(
          e,
          "Could not delete task path for task[%s]. This is not an error if httpRemote taskRunner is being used at overlord.",
          taskId
      );
    }
  }

  public void updateTaskStatusAnnouncement(TaskAnnouncement announcement) throws Exception
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      CuratorUtils.createOrSet(
          curatorFramework,
          getStatusPathForId(announcement.getTaskStatus().getId()),
          CreateMode.PERSISTENT,
          jsonMapper.writeValueAsBytes(announcement),
          config.getMaxZnodeBytes()
      );
    }
  }

  public List<TaskAnnouncement> getAnnouncements() throws Exception
  {
    final List<TaskAnnouncement> announcements = new ArrayList<>();

    for (String id : curatorFramework.getChildren().forPath(getStatusPathForWorker())) {
      announcements.add(
          jsonMapper.readValue(
              curatorFramework.getData().forPath(getStatusPathForId(id)),
              TaskAnnouncement.class
          )
      );
    }

    return announcements;
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
