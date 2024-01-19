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

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.utils.ServletResourceUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.joda.time.Interval;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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
  private final CoordinatorConfigManager manager;
  private final AuditManager auditManager;

  @Inject
  public CoordinatorDynamicConfigsResource(
      CoordinatorConfigManager manager,
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
    return Response.ok(manager.getCurrentDynamicConfig()).build();
  }

  // default value is used for backwards compatibility
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDynamicConfigs(
      final CoordinatorDynamicConfig.Builder dynamicConfigBuilder,
      @Context HttpServletRequest req
  )
  {
    try {
      CoordinatorDynamicConfig current = manager.getCurrentDynamicConfig();

      final SetResult setResult = manager.setDynamicConfig(
          dynamicConfigBuilder.build(current),
          AuthorizationUtils.buildAuditInfo(req)
      );

      if (setResult.isOk()) {
        return Response.ok().build();
      } else {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(setResult.getException()))
                       .build();
      }
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ServletResourceUtils.sanitizeException(e))
                     .build();
    }
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceRuleHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    if (theInterval == null && count != null) {
      try {
        return Response.ok(
            auditManager.fetchAuditHistory(
                CoordinatorDynamicConfig.CONFIG_KEY,
                CoordinatorDynamicConfig.CONFIG_KEY,
                count
            )
        ).build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ServletResourceUtils.sanitizeException(e))
                       .build();
      }
    }
    return Response.ok(
        auditManager.fetchAuditHistory(
            CoordinatorDynamicConfig.CONFIG_KEY,
            CoordinatorDynamicConfig.CONFIG_KEY,
            theInterval
        )
    ).build();
  }

}
