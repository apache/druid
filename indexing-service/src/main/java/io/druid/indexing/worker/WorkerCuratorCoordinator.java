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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
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

    this.announcer = new Announcer(curatorFramework, MoreExecutors.sameThreadExecutor());

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
      announcer.announce(getAnnouncementsPathForWorker(), jsonMapper.writeValueAsBytes(worker), false);

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

  public void announceTaskAnnouncement(TaskAnnouncement announcement)
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
                        .withMode(CreateMode.PERSISTENT)
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
          announceTaskAnnouncement(announcement);
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

  public List<TaskAnnouncement> getAnnouncements(){
    try {
      return Lists.transform(
          curatorFramework.getChildren().forPath(getStatusPathForWorker()), new Function<String, TaskAnnouncement>()
          {
            @Nullable
            @Override
            public TaskAnnouncement apply(String input)
            {
              try {
                return jsonMapper.readValue(curatorFramework.getData().forPath(getStatusPathForId(input)),TaskAnnouncement.class);
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
