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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 */
public class NoopTask extends AbstractTask
{
  private static final int DEFAULT_RUN_TIME = 2500;
  private static final int DEFAULT_IS_READY_TIME = 0;
  private static final IsReadyResult DEFAULT_IS_READY_RESULT = IsReadyResult.YES;

  enum IsReadyResult
  {
    YES,
    NO,
    EXCEPTION
  }

  @JsonIgnore
  private final long runTime;

  @JsonIgnore
  private final long isReadyTime;

  @JsonIgnore
  private final IsReadyResult isReadyResult;

  @JsonCreator
  public NoopTask(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") String groupId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("runTime") long runTimeMillis,
      @JsonProperty("isReadyTime") long isReadyTime,
      @JsonProperty("isReadyResult") String isReadyResult,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? StringUtils.format("noop_%s_%s", DateTimes.nowUtc(), UUID.randomUUID().toString()) : id,
        groupId,
        null,
        dataSource == null ? "none" : dataSource,
        context
    );

    this.runTime = (runTimeMillis == 0) ? DEFAULT_RUN_TIME : runTimeMillis;
    this.isReadyTime = (isReadyTime == 0) ? DEFAULT_IS_READY_TIME : isReadyTime;
    this.isReadyResult = (isReadyResult == null)
                         ? DEFAULT_IS_READY_RESULT
                         : IsReadyResult.valueOf(StringUtils.toUpperCase(isReadyResult));
  }

  @Override
  public String getType()
  {
    return "noop";
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return ImmutableSet.of();
  }

  @JsonProperty
  public long getRunTime()
  {
    return runTime;
  }

  @JsonProperty
  public long getIsReadyTime()
  {
    return isReadyTime;
  }

  @JsonProperty
  public IsReadyResult getIsReadyResult()
  {
    return isReadyResult;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    switch (isReadyResult) {
      case YES:
        return true;
      case NO:
        return false;
      case EXCEPTION:
        throw new ISE("Not ready. Never will be ready. Go away!");
      default:
        throw new AssertionError("#notreached");
    }
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    Thread.sleep(runTime);
    return TaskStatus.success(getId());
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  public static NoopTask create()
  {
    return forDatasource(null);
  }

  public static NoopTask forDatasource(String datasource)
  {
    return new NoopTask(null, null, datasource, 0, 0, null, null);
  }

  public static NoopTask withPriority(int priority)
  {
    final Map<String, Object> context = new HashMap<>();
    context.put(Tasks.PRIORITY_KEY, priority);
    return new NoopTask(null, null, null, 0, 0, null, context);
  }
}
