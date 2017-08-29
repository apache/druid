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

package io.druid.server.http.security;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.java.util.common.StringUtils;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Use this ResourceFilter at end points where Druid Cluster State is read or written
 * Here are some example paths where this filter is used -
 * - druid/broker/v1
 * - druid/coordinator/v1
 * - druid/historical/v1
 * - druid/indexer/v1
 * - druid/coordinator/v1/rules
 * - druid/coordinator/v1/tiers
 * - druid/worker/v1
 * - druid/coordinator/v1/servers
 * - status
 * Note - Currently the resource name for all end points is set to "STATE" however if more fine grained access control
 * is required the resource name can be set to specific state properties.
 */
public class StateResourceFilter extends AbstractResourceFilter
{
  @Inject
  public StateResourceFilter(AuthConfig authConfig)
  {
    super(authConfig);
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    if (getAuthConfig().isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final String resourceName = "STATE";
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) getReq().getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );

      final Access authResult = authorizationInfo.isAuthorized(
          new Resource(resourceName, ResourceType.STATE),
          getAction(request)
      );
      if (!authResult.isAllowed()) {
        throw new WebApplicationException(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }

    return request;
  }

  @Override
  public boolean isApplicable(String requestPath)
  {
    return requestPath.startsWith("druid/broker/v1") ||
           requestPath.startsWith("druid/coordinator/v1") ||
           requestPath.startsWith("druid/historical/v1") ||
           requestPath.startsWith("druid/indexer/v1") ||
           requestPath.startsWith("druid/coordinator/v1/rules") ||
           requestPath.startsWith("druid/coordinator/v1/tiers") ||
           requestPath.startsWith("druid/worker/v1") ||
           requestPath.startsWith("druid/coordinator/v1/servers") ||
           requestPath.startsWith("druid/v2") ||
           requestPath.startsWith("status");
  }
}
