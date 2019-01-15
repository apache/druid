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

package org.apache.druid.indexing.overlord.http.security;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.security.AbstractResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

/**
 * Use this ResourceFilter when the datasource information is present after "task" segment in the request Path
 * Here are some example paths where this filter is used -
 * - druid/indexer/v1/task/{taskid}/...
 * Note - DO NOT use this filter at MiddleManager resources as TaskStorageQueryAdapter cannot be injected there
 */
public class TaskResourceFilter extends AbstractResourceFilter
{
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;

  @Inject
  public TaskResourceFilter(
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      AuthorizerMapper authorizerMapper
  )
  {
    super(authorizerMapper);
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    String taskId = Preconditions.checkNotNull(
        request.getPathSegments()
               .get(
                   Iterables.indexOf(
                       request.getPathSegments(),
                       new Predicate<PathSegment>()
                       {
                         @Override
                         public boolean apply(PathSegment input)
                         {
                           return "task".equals(input.getPath());
                         }
                       }
                   ) + 1
               ).getPath()
    );
    taskId = StringUtils.urlDecode(taskId);

    Optional<Task> taskOptional = taskStorageQueryAdapter.getTask(taskId);
    if (!taskOptional.isPresent()) {
      throw new WebApplicationException(
          Response.status(Response.Status.BAD_REQUEST)
                  .entity(StringUtils.format("Cannot find any task with id: [%s]", taskId))
                  .build()
      );
    }
    final String dataSourceName = Preconditions.checkNotNull(taskOptional.get().getDataSource());

    final ResourceAction resourceAction = new ResourceAction(
        new Resource(dataSourceName, ResourceType.DATASOURCE),
        getAction(request)
    );

    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        getReq(),
        resourceAction,
        getAuthorizerMapper()
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    return request;
  }
}
