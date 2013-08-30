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

package com.metamx.druid.indexing.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.druid.indexing.common.tasklogs.TaskLogPusher;
import com.metamx.druid.indexing.coordinator.config.ForkingTaskRunnerConfig;
import io.druid.guice.guice.annotations.Self;
import io.druid.server.DruidNode;

import java.util.Properties;

/**
*/
public class ForkingTaskRunnerFactory implements TaskRunnerFactory
{
  private final ForkingTaskRunnerConfig config;
  private final Properties props;
  private final ObjectMapper jsonMapper;
  private final TaskLogPusher persistentTaskLogs;
  private final DruidNode node;

  @Inject
  public ForkingTaskRunnerFactory(
      final ForkingTaskRunnerConfig config,
      final Properties props,
      final ObjectMapper jsonMapper,
      final TaskLogPusher persistentTaskLogs,
      @Self DruidNode node
  ) {
    this.config = config;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.persistentTaskLogs = persistentTaskLogs;
    this.node = node;
  }

  @Override
  public TaskRunner build()
  {
    return new ForkingTaskRunner(config, props, persistentTaskLogs, jsonMapper, node);
  }
}
