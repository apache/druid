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

package org.apache.druid.indexing.overlord.sampler;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.HashSet;
import java.util.Set;

@Path("/druid/indexer/v1/sampler")
public class SamplerResource
{
  private final AuthorizerMapper authorizerMapper;
  private final AuthConfig authConfig;
  private static final ResourceAction STATE_RESOURCE_WRITE =
      new ResourceAction(Resource.STATE_RESOURCE, Action.WRITE);

  @Inject
  public SamplerResource(
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.authConfig = authConfig;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public SamplerResponse post(final SamplerSpec sampler, @Context final HttpServletRequest req)
  {
    Preconditions.checkNotNull(sampler, "Request body cannot be empty");
    Set<ResourceAction> resourceActions = new HashSet<>();
    resourceActions.add(STATE_RESOURCE_WRITE);
    if (authConfig.isEnableInputSourceSecurity()) {
      resourceActions.addAll(sampler.getInputSourceResources());
    }

    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        resourceActions,
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.getMessage());
    }
    return sampler.sample();
  }
}
