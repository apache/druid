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

package io.druid.indexing.common.task;

import io.druid.indexing.common.TaskStatus;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import io.druid.utils.CircularBuffer;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public class IndexTaskUtils
{
  @Nullable
  public static List<String> getMessagesFromSavedParseExceptions(CircularBuffer<Throwable> savedParseExceptions)
  {
    if (savedParseExceptions == null) {
      return null;
    }

    List<String> events = new ArrayList<>();
    for (int i = 0; i < savedParseExceptions.size(); i++) {
      events.add(savedParseExceptions.getLatest(i).getMessage());
    }

    return events;
  }

  /**
   * Authorizes action to be performed on a task's datasource
   *
   * @return authorization result
   */
  public static Access datasourceAuthorizationCheck(
      final HttpServletRequest req,
      Action action,
      String datasource,
      AuthorizerMapper authorizerMapper
  )
  {
    ResourceAction resourceAction = new ResourceAction(
        new Resource(datasource, ResourceType.DATASOURCE),
        action
    );

    Access access = AuthorizationUtils.authorizeResourceAction(req, resourceAction, authorizerMapper);
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }

    return access;
  }

  public static void setTaskDimensions(final ServiceMetricEvent.Builder metricBuilder, final Task task)
  {
    metricBuilder.setDimension(DruidMetrics.TASK_ID, task.getId());
    metricBuilder.setDimension(DruidMetrics.TASK_TYPE, task.getType());
    metricBuilder.setDimension(DruidMetrics.DATASOURCE, task.getDataSource());
  }

  public static void setTaskStatusDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final TaskStatus taskStatus
  )
  {
    metricBuilder.setDimension(DruidMetrics.TASK_ID, taskStatus.getId());
    metricBuilder.setDimension(DruidMetrics.TASK_STATUS, taskStatus.getStatusCode().toString());
  }
}
