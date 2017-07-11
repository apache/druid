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

package io.druid.indexing.overlord.http.security;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.java.util.common.StringUtils;
import io.druid.server.http.security.AbstractResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import java.util.List;

public class SupervisorResourceFilter extends AbstractResourceFilter
{
  private final SupervisorManager supervisorManager;

  @Inject
  public SupervisorResourceFilter(AuthConfig authConfig, SupervisorManager supervisorManager)
  {
    super(authConfig);
    this.supervisorManager = supervisorManager;
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (getAuthConfig().isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
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
                             return input.getPath().equals("supervisor");
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

      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) getReq().getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );

      final SupervisorSpec spec = supervisorSpecOptional.get();
      Preconditions.checkArgument(
          spec.getDataSources() != null && spec.getDataSources().size() > 0,
          "No dataSources found to perform authorization checks"
      );

      for (String dataSource : spec.getDataSources()) {
        Access authResult = authorizationInfo.isAuthorized(
            new Resource(dataSource, ResourceType.DATASOURCE),
            getAction(request)
        );
        if (!authResult.isAllowed()) {
          throw new WebApplicationException(Response.status(Response.Status.FORBIDDEN)
                                                    .entity(
                                                        StringUtils.format("Access-Check-Result: %s", authResult.toString())
                                                    )
                                                    .build());
        }
      }
    }

    return request;
  }

  @Override
  public boolean isApplicable(String requestPath)
  {
    List<String> applicablePaths = ImmutableList.of("druid/indexer/v1/supervisor/");
    for (String path : applicablePaths) {
      if (requestPath.startsWith(path) && !requestPath.equals(path)) {
        return true;
      }
    }
    return false;
  }

}
