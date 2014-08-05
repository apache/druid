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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;

import java.util.Properties;

/**
*/
public class ForkingTaskRunnerFactory implements TaskRunnerFactory
{
  private final ForkingTaskRunnerConfig config;
  private final TaskConfig taskConfig;
  private final WorkerConfig workerConfig;
  private final Properties props;
  private final ObjectMapper jsonMapper;
  private final TaskLogPusher persistentTaskLogs;
  private final DruidNode node;

  @Inject
  public ForkingTaskRunnerFactory(
      final ForkingTaskRunnerConfig config,
      final TaskConfig taskConfig,
      final WorkerConfig workerConfig,
      final Properties props,
      final ObjectMapper jsonMapper,
      final TaskLogPusher persistentTaskLogs,
      @Self DruidNode node
  ) {
    this.config = config;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.persistentTaskLogs = persistentTaskLogs;
    this.node = node;
  }

  @Override
  public TaskRunner build()
  {
    return new ForkingTaskRunner(config, taskConfig, workerConfig, props, persistentTaskLogs, jsonMapper, node);
  }
}
