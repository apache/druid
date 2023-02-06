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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.druid.annotations.UsedInGeneratedCode;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Holds information about a worker and a listener for task status changes associated with the worker.
 */
public class ZkWorker implements Closeable
{
  private final PathChildrenCache statusCache;
  private final Function<ChildData, TaskAnnouncement> cacheConverter;
  private final java.util.function.Function<ChildData, String> taskIdExtractor;

  private AtomicReference<Worker> worker;
  private AtomicReference<DateTime> lastCompletedTaskTime = new AtomicReference<>(DateTimes.nowUtc());
  private AtomicReference<DateTime> blacklistedUntil = new AtomicReference<>();
  private AtomicInteger continuouslyFailedTasksCount = new AtomicInteger(0);

  public ZkWorker(Worker worker, PathChildrenCache statusCache, final ObjectMapper jsonMapper)
  {
    this.worker = new AtomicReference<>(worker);
    this.statusCache = statusCache;
    this.cacheConverter = (ChildData input) ->
        JacksonUtils.readValue(jsonMapper, input.getData(), TaskAnnouncement.class);
    this.taskIdExtractor = createTaskIdExtractor(jsonMapper);
  }

  static java.util.function.Function<ChildData, String> createTaskIdExtractor(final ObjectMapper jsonMapper)
  {
    return (ChildData input) -> {
      try (JsonParser parser = jsonMapper.getFactory().createParser(input.getData())) {
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          String currentName = parser.getCurrentName();
          if (currentName == null) {
            continue;
          }

          switch (currentName) {
            case TaskAnnouncement.TASK_ID_KEY:
              parser.nextToken();
              return parser.getValueAsString();
            default:
              parser.skipChildren();
          }
        }
        return null;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public void start() throws Exception
  {
    statusCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
  }

  public void addListener(PathChildrenCacheListener listener)
  {
    statusCache.getListenable().addListener(listener);
  }

  @JsonProperty("worker")
  public Worker getWorker()
  {
    return worker.get();
  }

  @JsonProperty("runningTasks")
  public Collection<String> getRunningTaskIds()
  {
    return statusCache.getCurrentData()
        .stream()
        .map(taskIdExtractor)
        .collect(Collectors.toSet());
  }

  public Map<String, TaskAnnouncement> getRunningTasks()
  {
    Map<String, TaskAnnouncement> retVal = new HashMap<>();
    for (TaskAnnouncement taskAnnouncement : Lists.transform(
        statusCache.getCurrentData(),
        cacheConverter
    )) {
      retVal.put(taskAnnouncement.getTaskStatus().getId(), taskAnnouncement);
    }

    return retVal;
  }

  @JsonProperty("currCapacityUsed")
  public int getCurrCapacityUsed()
  {
    return getCurrCapacityUsed(getRunningTasks());
  }

  private static int getCurrCapacityUsed(Map<String, TaskAnnouncement> tasks)
  {
    int currCapacity = 0;
    for (TaskAnnouncement taskAnnouncement : tasks.values()) {
      currCapacity += taskAnnouncement.getTaskResource().getRequiredCapacity();
    }
    return currCapacity;
  }

  @JsonProperty("currParallelIndexCapacityUsed")
  public int getCurrParallelIndexCapacityUsed()
  {
    return getCurrParallelIndexCapacityUsed(getRunningTasks());
  }

  private int getCurrParallelIndexCapacityUsed(Map<String, TaskAnnouncement> tasks)
  {
    int currParallelIndexCapacityUsed = 0;
    for (TaskAnnouncement taskAnnouncement : tasks.values()) {
      if (taskAnnouncement.getTaskType().equals(ParallelIndexSupervisorTask.TYPE)) {
        currParallelIndexCapacityUsed += taskAnnouncement.getTaskResource().getRequiredCapacity();
      }
    }
    return currParallelIndexCapacityUsed;
  }

  @JsonProperty("availabilityGroups")
  public Set<String> getAvailabilityGroups()
  {
    return getAvailabilityGroups(getRunningTasks());
  }

  private static Set<String> getAvailabilityGroups(Map<String, TaskAnnouncement> tasks)
  {
    Set<String> retVal = new HashSet<>();
    for (TaskAnnouncement taskAnnouncement : tasks.values()) {
      retVal.add(taskAnnouncement.getTaskResource().getAvailabilityGroup());
    }
    return retVal;
  }

  @JsonProperty
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime.get();
  }

  @JsonProperty
  public DateTime getBlacklistedUntil()
  {
    return blacklistedUntil.get();
  }

  public boolean isRunningTask(String taskId)
  {
    return statusCache.getCurrentData()
        .stream()
        .map(taskIdExtractor)
        .anyMatch((String s) -> taskId.equals(s));
  }

  @UsedInGeneratedCode // See JavaScriptWorkerSelectStrategyTest
  public boolean isValidVersion(String minVersion)
  {
    return worker.get().getVersion().compareTo(minVersion) >= 0;
  }

  public void setWorker(Worker newWorker)
  {
    final Worker oldWorker = worker.get();
    Preconditions.checkArgument(newWorker.getHost().equals(oldWorker.getHost()), "Cannot change Worker host");
    Preconditions.checkArgument(newWorker.getIp().equals(oldWorker.getIp()), "Cannot change Worker ip");

    worker.set(newWorker);
  }

  public void setLastCompletedTaskTime(DateTime completedTaskTime)
  {
    lastCompletedTaskTime.set(completedTaskTime);
  }

  public void setBlacklistedUntil(DateTime blacklistedUntil)
  {
    this.blacklistedUntil.set(blacklistedUntil);
  }

  public ImmutableWorkerInfo toImmutable()
  {
    Map<String, TaskAnnouncement> tasks = getRunningTasks();

    return new ImmutableWorkerInfo(
        worker.get(),
        getCurrCapacityUsed(tasks),
        getCurrParallelIndexCapacityUsed(tasks),
        getAvailabilityGroups(tasks),
        tasks.keySet(),
        lastCompletedTaskTime.get(),
        blacklistedUntil.get()
    );
  }

  @Override
  public void close() throws IOException
  {
    statusCache.close();
  }

  public int getContinuouslyFailedTasksCount()
  {
    return continuouslyFailedTasksCount.get();
  }

  public void resetContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.set(0);
  }

  public void incrementContinuouslyFailedTasksCount()
  {
    this.continuouslyFailedTasksCount.incrementAndGet();
  }

  @Override
  public String toString()
  {
    return "ZkWorker{" +
           "worker=" + worker +
           ", lastCompletedTaskTime=" + lastCompletedTaskTime +
           ", blacklistedUntil=" + blacklistedUntil +
           '}';
  }
}
