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

package io.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.config.ConfigManager.SetResult;
import io.druid.common.config.JacksonConfigManager;
import io.druid.java.util.common.StringUtils;
import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.server.coordinator.DataSourceCompactionConfig;
import io.druid.server.http.security.ConfigResourceFilter;

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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Path("/druid/coordinator/v1/config/compaction")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorCompactionConfigsResource
{
  private final JacksonConfigManager manager;

  @Inject
  public CoordinatorCompactionConfigsResource(JacksonConfigManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactConfig()
  {
    return Response.ok(
        manager.watch(
            CoordinatorCompactionConfig.CONFIG_KEY,
            CoordinatorCompactionConfig.class
        ).get()
    ).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setCompactionTaskLimit(
      @QueryParam("slotRatio") Double compactionTaskSlotRatio,
      @QueryParam("maxSlots") Integer maxCompactionTaskSlots,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    CoordinatorCompactionConfig current = manager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class
    ).get();

    final CoordinatorCompactionConfig newCompactionConfig;
    if (current != null) {
      newCompactionConfig = CoordinatorCompactionConfig.from(current, compactionTaskSlotRatio, maxCompactionTaskSlots);
    } else {
      newCompactionConfig = new CoordinatorCompactionConfig(null, compactionTaskSlotRatio, maxCompactionTaskSlots);
    }

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
  @Path("/{dataSource}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addOrUpdateCompactionConfig(
      final DataSourceCompactionConfig newConfig,
      @PathParam("dataSource") String dataSource,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context HttpServletRequest req
  )
  {
    if (!dataSource.equals(newConfig.getDataSource())) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(
              StringUtils.format(
                  "dataSource[%s] in config is different from the requested one[%s]",
                  newConfig.getDataSource(),
                  dataSource
              )
          )
          .build();
    }

    CoordinatorCompactionConfig current = manager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class
    ).get();

    final CoordinatorCompactionConfig newCompactionConfig;
    if (current != null) {
      final Map<String, DataSourceCompactionConfig> newConfigs = current
          .getCompactionConfigs()
          .stream()
          .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
      newConfigs.put(dataSource, newConfig);
      newCompactionConfig = CoordinatorCompactionConfig.from(current, ImmutableList.copyOf(newConfigs.values()));
    } else {
      newCompactionConfig = CoordinatorCompactionConfig.from(ImmutableList.of(newConfig));
    }

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
  public Response getCompactionConfig(@PathParam("dataSource") String dataSource)
  {
    CoordinatorCompactionConfig current = manager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class
    ).get();

    if (current == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

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
    CoordinatorCompactionConfig current = manager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class
    ).get();

    if (current == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

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
}
