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

package org.apache.druid.cli;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.metrics.MetricsModule;

import java.util.Map;

/**
 * TaskHolder implementation for {@code CliPeon} processes.
 *
 * <p>This holder retrieves task information lazily via a {@link Provider} to avoid cyclic dependencies
 * during Guice initialization. A {@link Task} may include a {@link TransformSpec}, an {@link AggregatorFactory},
 * or an {@link AbstractMonitor}, all of which require modules such as {@link LookupModule} or {@link MetricsModule}.
 * Since those modules depend on this holder, using a Provider breaks the dependency cycle by binding it lazily.
 * </p>
 */
public class PeonTaskHolder implements TaskHolder
{
  private final Provider<Task> taskProvider;

  @Inject
  public PeonTaskHolder(final Provider<Task> taskProvider)
  {
    this.taskProvider = taskProvider;
  }

  @Override
  public String getDataSource()
  {
    return taskProvider.get().getDataSource();
  }

  @Override
  public String getTaskId()
  {
    return taskProvider.get().getId();
  }

  @Override
  public String getTaskType()
  {
    return taskProvider.get().getType();
  }

  @Override
  public String getGroupId()
  {
    return taskProvider.get().getGroupId();
  }

  /**
   * @return a map of all task-specific dimensions applicable to this peon.
   * The task ID ({@link TaskHolder#getTaskId()}) is added to both {@link DruidMetrics#TASK_ID}
   * {@link DruidMetrics#ID} dimensions to the map for backward compatibility. {@link DruidMetrics#ID} is
   * deprecated because it's ambiguous and can be removed in a future release.</p>
   */
  @Override
  public Map<String, String> getMetricDimensions()
  {
    return Map.of(
        DruidMetrics.DATASOURCE, getDataSource(),
        DruidMetrics.TASK_ID, getTaskId(),
        DruidMetrics.ID, getTaskId(),
        DruidMetrics.TASK_TYPE, getTaskType(),
        DruidMetrics.GROUP_ID, getGroupId()
    );
  }
}
