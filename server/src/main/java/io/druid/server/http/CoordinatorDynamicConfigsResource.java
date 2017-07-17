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

package io.druid.server.http;

import com.google.common.collect.ImmutableMap;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.config.JacksonConfigManager;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
import io.druid.server.http.security.ConfigResourceFilter;
import org.joda.time.Interval;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 */
@Path("/druid/coordinator/v1/config")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorDynamicConfigsResource
{
  private final JacksonConfigManager manager;
  private final AuditManager auditManager;

  @Inject
  public CoordinatorDynamicConfigsResource(
      JacksonConfigManager manager,
      AuditManager auditManager
  )
  {
    this.manager = manager;
    this.auditManager = auditManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDynamicConfigs()
  {
    return Response.ok(
        manager.watch(
            CoordinatorDynamicConfig.CONFIG_KEY,
            CoordinatorDynamicConfig.class
        ).get()
    ).build();
  }

  // default value is used for backwards compatibility
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDynamicConfigs(final CoordinatorDynamicConfig.Builder dynamicConfigBuilder,
                                    @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
                                    @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
                                    @Context HttpServletRequest req
  )
  {
    CoordinatorDynamicConfig current = manager.watch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        CoordinatorDynamicConfig.class
    ).get();

    if (!manager.set(
        CoordinatorDynamicConfig.CONFIG_KEY,
        current == null ? dynamicConfigBuilder.build() : dynamicConfigBuilder.build(current),
        new AuditInfo(author, comment, req.getRemoteAddr())
    )) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceRuleHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : new Interval(interval);
    if (theInterval == null && count != null) {
      try {
        return Response.ok(
            auditManager.fetchAuditHistory(
                CoordinatorDynamicConfig.CONFIG_KEY,
                CoordinatorDynamicConfig.CONFIG_KEY,
                count
            )
        )
                       .build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
    }
    return Response.ok(
        auditManager.fetchAuditHistory(
            CoordinatorDynamicConfig.CONFIG_KEY,
            CoordinatorDynamicConfig.CONFIG_KEY,
            theInterval
        )
    )
                   .build();
  }

}
