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
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path("/druid/indexer/v1/sampler")
public class SamplerResource
{
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SamplerResource(AuthorizerMapper authorizerMapper)
  {
    this.authorizerMapper = authorizerMapper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public SamplerResponse post(final SamplerSpec sampler, @Context final HttpServletRequest req)
  {
    if (authorizerMapper.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      final List<ResourceAction> resourceActions = new ArrayList<>();
      resourceActions.add(new ResourceAction(Resource.SERVER_USER_RESOURCE, Action.WRITE));
      // make sure the user has read permissions on the druid datasource
      if (sampler instanceof IndexTaskSamplerSpec
          && ((IndexTaskSamplerSpec) sampler).getInputSource() != null
          && ((IndexTaskSamplerSpec) sampler).getInputSource() instanceof DruidInputSource) {
        final String dataSource = ((DruidInputSource) ((IndexTaskSamplerSpec) sampler).getInputSource())
            .getDataSource();
        resourceActions.add(new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ));
      }
      final Access authResult = AuthorizationUtils.authorizeAllResourceActions(
          req,
          resourceActions,
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.getMessage());
      }
    }
    return Preconditions.checkNotNull(sampler, "Request body cannot be empty").sample();
  }
}
