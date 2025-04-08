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

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/coordinator/v1/config/compaction")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorCompactionConfigsResource
{
  private final CoordinatorConfigManager configManager;

  @Inject
  public CoordinatorCompactionConfigsResource(
      CoordinatorConfigManager configManager
  )
  {
    this.configManager = configManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactionConfig()
  {
    return ServletResourceUtils.buildReadResponse(
        configManager::getCurrentCompactionConfig
    );
  }

  /**
   * @deprecated Use API {@code GET /druid/indexer/v1/compaction/config/cluster} instead.
   */
  @POST
  @Deprecated
  @Path("/taskslots")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setCompactionTaskLimit(
      @QueryParam("ratio") Double compactionTaskSlotRatio,
      @QueryParam("max") Integer maxCompactionTaskSlots,
      @Context HttpServletRequest req
  )
  {
    if (compactionTaskSlotRatio == null && maxCompactionTaskSlots == null) {
      return ServletResourceUtils.buildUpdateResponse(() -> true);
    }

    final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(req);
    return ServletResourceUtils.buildUpdateResponse(
        () -> configManager.updateCompactionTaskSlots(compactionTaskSlotRatio, maxCompactionTaskSlots, auditInfo)
    );
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response addOrUpdateDatasourceCompactionConfig(
      final DataSourceCompactionConfig newConfig,
      @Context HttpServletRequest req
  )
  {
    final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(req);
    return ServletResourceUtils.buildUpdateResponse(() -> {
      if (newConfig.getEngine() == CompactionEngine.MSQ) {
        throw InvalidInput.exception(
            "MSQ engine is supported only with supervisor-based compaction on the Overlord."
        );
      }
      return configManager.updateDatasourceCompactionConfig(newConfig, auditInfo);
    });
  }

  @GET
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceCompactionConfig(@PathParam("dataSource") String dataSource)
  {
    return ServletResourceUtils.buildReadResponse(
        () -> configManager.getDatasourceCompactionConfig(dataSource)
    );
  }

  @GET
  @Path("/{dataSource}/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactionConfigHistory(
      @PathParam("dataSource") String dataSource,
      @QueryParam("interval") String interval,
      @QueryParam("count") Integer count
  )
  {
    return ServletResourceUtils.buildReadResponse(
        () -> configManager.getCompactionConfigHistory(dataSource, interval, count)
    );
  }

  @DELETE
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteCompactionConfig(
      @PathParam("dataSource") String dataSource,
      @Context HttpServletRequest req
  )
  {
    final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(req);
    return ServletResourceUtils.buildUpdateResponse(
        () -> configManager.deleteDatasourceCompactionConfig(dataSource, auditInfo)
    );
  }
}
