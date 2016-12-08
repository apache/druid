/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.http.client.HttpClient;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.java.util.common.logger.Logger;
import org.apache.curator.x.discovery.ServiceDiscovery;

public class DynamicUpstreamReporter implements TaskStatusReporter
{
  private static final Logger LOG = new Logger(DynamicUpstreamReporter.class);
  @Inject(optional = true)
  private TaskMaster taskMaster = null;
  @Inject
  private
  @Global
  HttpClient httpClient = null;
  @Inject
  private ServiceDiscovery<Void> discovery = null;
  @Inject
  private
  @Named(TaskTierModule.UPSTREAM_SERVICE_NAME_CONSTANT_KEY)
  String upstreamService = null;

  public DynamicUpstreamReporter() {}

  public DynamicUpstreamReporter(
      TaskMaster taskMaster,
      @Global HttpClient httpClient,
      ServiceDiscovery<Void> discovery,
      @Named(TaskTierModule.UPSTREAM_SERVICE_NAME_CONSTANT_KEY) String upstreamService
  )
  {
    this.taskMaster = taskMaster;
    this.httpClient = httpClient;
    this.discovery = discovery;
    this.upstreamService = upstreamService;
  }

  @Override
  public boolean reportStatus(TaskStatus status)
  {
    TaskStatusReporter reporter = new TaskStatusPostToLeaderReporter(httpClient, discovery, upstreamService);
    if (taskMaster != null) {
      final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
      if (taskRunnerOptional.isPresent()) {
        final TaskRunner runner = taskRunnerOptional.get();
        if (runner instanceof TaskStatusReporter) {
          reporter = (TaskStatusReporter) runner;
        } else {
          LOG.debug(
              "Expected [%s] but was [%s]. Trying POST",
              TaskStatusReporter.class,
              runner.getClass().getCanonicalName()
          );
        }
      } else {
        LOG.debug("No task runner. Trying POST");
      }
    } else {
      LOG.debug("No task master. Trying POST");
    }
    return reporter.reportStatus(status);
  }
}
