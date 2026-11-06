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

package org.apache.druid.server.http.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nullable;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

/**
 * Use this resource filter for lookup API endpoints. It authorizes routes with a lookup path parameter.
 * Resource methods authorize bulk routes.
 *
 * The resource name is {@code tier/lookup}, or {@code tier} when the tier has no lookups.
 * Percent signs and slashes in each component are escaped before joining them.
 * Coordinator-wide requests with no tier or lookup use {@code LOOKUP:LOOKUP}.
 * When lookup authorization is disabled, all requests use {@code CONFIG:CONFIG}.
 *
 * Here are some example paths where this filter is used -
 * - druid/coordinator/v1/lookups/config/{tier}/{lookup}
 * - druid/coordinator/v1/lookups/status/{tier}/{lookup}
 * - druid/v1/lookups/introspect/{lookupId}
 */
public class LookupResourceFilter extends AbstractResourceFilter
{
  private final boolean enabled;

  // Coordinator routes provide a tier in the path and do not bind LookupNodeService.
  @Inject(optional = true)
  LookupNodeService lookupNodeService;

  @Context
  UriInfo uriInfo;

  @Inject
  public LookupResourceFilter(AuthorizerMapper authorizerMapper, AuthConfig authConfig)
  {
    super(authorizerMapper);
    this.enabled = authConfig.isEnableLookupAuthorization();
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    final ResourceAction resourceAction;
    if (!enabled) {
      resourceAction = new ResourceAction(new Resource("CONFIG", ResourceType.CONFIG), getAction(request));
    } else {
      final MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
      final String lookup = lookupPathParam(pathParams);
      if (lookup == null) {
        return request;
      }
      final String tier = pathParams.getFirst("tier") == null
                          ? lookupNodeService.getLookupTier()
                          : pathParams.getFirst("tier");
      resourceAction = new ResourceAction(
          new Resource(resourceName(tier, lookup), ResourceType.LOOKUP),
          getAction(request)
      );
    }

    final AuthorizationResult authResult = AuthorizationUtils.authorizeResourceAction(
        getReq(), resourceAction, getAuthorizerMapper()
    );
    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
    }
    return request;
  }

  private static String lookupPathParam(MultivaluedMap<String, String> pathParams)
  {
    for (final String param : ImmutableList.of("lookup", "lookupId", "id")) {
      if (pathParams.containsKey(param)) {
        return pathParams.getFirst(param);
      }
    }
    return null;
  }

  /**
   * Returns the policy name for a lookup, an empty tier, or the coordinator-wide fallback.
   */
  public static String resourceName(@Nullable String tier, @Nullable String lookup)
  {
    return tier == null ? ResourceType.LOOKUP : lookup == null ? escape(tier) : escape(tier) + "/" + escape(lookup);
  }

  private static String escape(String value)
  {
    return StringUtils.replace(StringUtils.replace(value, "%", "%25"), "/", "%2F");
  }
}
