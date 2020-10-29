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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import net.thisptr.jackson.jq.internal.misc.Lists;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.ServerServerResourceFilter;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Path("/druid/coordinator/v1/config/compaction")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorCompactionConfigsResource
{
  private final JacksonConfigManager manager;
  private final AuthorizerMapper authorizerMapper;
  private final AuthConfig authConfig;

  @Inject
  public CoordinatorCompactionConfigsResource(
      JacksonConfigManager manager,
      AuthorizerMapper authorizerMapper,
      AuthConfig authConfig
  )
  {
    this.manager = manager;
    this.authorizerMapper = authorizerMapper;
    this.authConfig = authConfig;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactionConfig(@Context HttpServletRequest request)
  {
    if (authConfig.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      return Response
          .ok(getFilteredConfigsByDatasource(CoordinatorCompactionConfig.current(manager), request, Action.READ))
          .build();
    } else {
      return Response.ok(CoordinatorCompactionConfig.current(manager)).build();
    }
  }

  @POST
  @Path("/taskslots")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ServerServerResourceFilter.class)
  public Response setCompactionTaskLimit(
      @QueryParam("ratio") Double compactionTaskSlotRatio,
      @QueryParam("max") Integer maxCompactionTaskSlots,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.current(manager);

    final CoordinatorCompactionConfig newCompactionConfig = CoordinatorCompactionConfig.from(
        current,
        compactionTaskSlotRatio,
        maxCompactionTaskSlots
    );

    final SetResult setResult = manager.set(
        CoordinatorCompactionConfig.CONFIG_KEY,
        newCompactionConfig,
        new AuditInfo(author, comment, req.getRemoteAddr())
    );

    if (setResult.isOk()) {
      return Response.ok().build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", setResult.getException()))
                     .build();
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addOrUpdateCompactionConfig(
      final DataSourceCompactionConfig newConfig,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    if (authConfig.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      if (!AuthorizationUtils.authorizeResourceAction(req, AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR.apply(
          newConfig.getDataSource()), authorizerMapper).isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).build();
      }
    }
    final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.current(manager);
    final CoordinatorCompactionConfig newCompactionConfig;
    final Map<String, DataSourceCompactionConfig> newConfigs = current
        .getCompactionConfigs()
        .stream()
        .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
    newConfigs.put(newConfig.getDataSource(), newConfig);
    newCompactionConfig = CoordinatorCompactionConfig.from(current, ImmutableList.copyOf(newConfigs.values()));

    final SetResult setResult = manager.set(
        CoordinatorCompactionConfig.CONFIG_KEY,
        newCompactionConfig,
        new AuditInfo(author, comment, req.getRemoteAddr())
    );

    if (setResult.isOk()) {
      return Response.ok().build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  @GET
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactionConfig(@PathParam("dataSource") String dataSource, @Context HttpServletRequest req)
  {
    if (authConfig.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      if (!AuthorizationUtils.authorizeResourceAction(
          req,
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(dataSource),
          authorizerMapper
      ).isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).build();
      }
    }
    final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.current(manager);
    final Map<String, DataSourceCompactionConfig> configs = current
        .getCompactionConfigs()
        .stream()
        .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));

    final DataSourceCompactionConfig config = configs.get(dataSource);
    if (config == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.ok().entity(config).build();
  }

  @DELETE
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteCompactionConfig(
      @PathParam("dataSource") String dataSource,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    if (authConfig.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      if (!AuthorizationUtils.authorizeResourceAction(
          req,
          AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR.apply(dataSource),
          authorizerMapper
      ).isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).build();
      }
    }
    final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.current(manager);
    final Map<String, DataSourceCompactionConfig> configs = current
        .getCompactionConfigs()
        .stream()
        .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));

    final DataSourceCompactionConfig config = configs.remove(dataSource);
    if (config == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    final SetResult setResult = manager.set(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.from(current, ImmutableList.copyOf(configs.values())),
        new AuditInfo(author, comment, req.getRemoteAddr())
    );

    if (setResult.isOk()) {
      return Response.ok().build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  private CoordinatorCompactionConfig getFilteredConfigsByDatasource(
      CoordinatorCompactionConfig compactionConfig,
      HttpServletRequest request,
      Action action
  )
  {
    final Iterable<DataSourceCompactionConfig> dataSourceCompactionConfigs = AuthorizationUtils
        .filterAuthorizedResources(request, compactionConfig.getCompactionConfigs(),
            input -> Collections
                .singleton(new ResourceAction(new Resource(input.getDataSource(), ResourceType.DATASOURCE), action)),
            authorizerMapper
        );
    // remove so that it does not complain about it when checking for server server permission
    request.removeAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED);
    if (AuthorizationUtils
        .authorizeResourceAction(request, new ResourceAction(Resource.SERVER_SERVER_RESOURCE, action), authorizerMapper)
        .isAllowed()) {
      return CoordinatorCompactionConfig.from(compactionConfig, Lists.newArrayList(dataSourceCompactionConfigs));
    }
    return CoordinatorCompactionConfig.from(Lists.newArrayList(dataSourceCompactionConfigs));
  }
}
