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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.security.AbstractResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

public class SupervisorResourceFilter extends AbstractResourceFilter
{
  private final SupervisorManager supervisorManager;

  @Inject
  public SupervisorResourceFilter(
      AuthorizerMapper authorizerMapper,
      SupervisorManager supervisorManager
  )
  {
    super(authorizerMapper);
    this.supervisorManager = supervisorManager;
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    final String supervisorId = Preconditions.checkNotNull(
        request.getPathSegments()
               .get(
                   Iterables.indexOf(
                       request.getPathSegments(),
                       new Predicate<PathSegment>()
                       {
                         @Override
                         public boolean apply(PathSegment input)
                         {
                           return "supervisor".equals(input.getPath());
                         }
                       }
                   ) + 1
               ).getPath()
    );

    Optional<SupervisorSpec> supervisorSpecOptional = supervisorManager.getSupervisorSpec(supervisorId);
    if (!supervisorSpecOptional.isPresent()) {
      throw new WebApplicationException(
          Response.status(Response.Status.BAD_REQUEST)
                  .entity(StringUtils.format("Cannot find any supervisor with id: [%s]", supervisorId))
                  .build()
      );
    }


    final SupervisorSpec spec = supervisorSpecOptional.get();
    Preconditions.checkArgument(
        spec.getDataSources() != null && spec.getDataSources().size() > 0,
        "No dataSources found to perform authorization checks"
    );

    Function<String, ResourceAction> resourceActionFunction = getAction(request) == Action.READ ?
                                                              AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR :
                                                              AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR;

    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        getReq(),
        Iterables.transform(spec.getDataSources(), resourceActionFunction),
        getAuthorizerMapper()
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    return request;
  }
}
