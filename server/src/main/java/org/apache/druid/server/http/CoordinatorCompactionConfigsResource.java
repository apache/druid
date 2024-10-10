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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigHistory;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.joda.time.Interval;

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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.UnaryOperator;

@Path("/druid/coordinator/v1/config/compaction")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorCompactionConfigsResource
{
  private static final Logger LOG = new Logger(CoordinatorCompactionConfigsResource.class);
  private static final long UPDATE_RETRY_DELAY = 1000;
  static final int MAX_UPDATE_RETRIES = 5;

  private final CoordinatorConfigManager configManager;
  private final AuditManager auditManager;

  @Inject
  public CoordinatorCompactionConfigsResource(
      CoordinatorConfigManager configManager,
      AuditManager auditManager
  )
  {
    this.configManager = configManager;
    this.auditManager = auditManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompactionConfig()
  {
    return Response.ok(configManager.getCurrentCompactionConfig()).build();
  }

  @POST
  @Path("/cluster")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateClusterCompactionConfig(
      ClusterCompactionConfig updatePayload,
      @Context HttpServletRequest req
  )
  {
    UnaryOperator<DruidCompactionConfig> operator = current -> current.withClusterConfig(updatePayload);
    return updateConfigHelper(operator, AuthorizationUtils.buildAuditInfo(req));
  }

  /**
   * @deprecated in favor of {@link #updateClusterCompactionConfig}.
   */
  @POST
  @Deprecated
  @Path("/taskslots")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setCompactionTaskLimit(
      @QueryParam("ratio") Double compactionTaskSlotRatio,
      @QueryParam("max") Integer maxCompactionTaskSlots,
      @QueryParam("useAutoScaleSlots") Boolean useAutoScaleSlots,
      @Context HttpServletRequest req
  )
  {
    return updateClusterCompactionConfig(
        new ClusterCompactionConfig(
            compactionTaskSlotRatio,
            maxCompactionTaskSlots,
            useAutoScaleSlots,
            null
        ),
        req
    );
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response addOrUpdateDatasourceCompactionConfig(
      final DataSourceCompactionConfig newConfig,
      @Context HttpServletRequest req
  )
  {
    UnaryOperator<DruidCompactionConfig> callable = current -> {
      if (newConfig.getEngine() == CompactionEngine.MSQ) {
        throw InvalidInput.exception(
            "MSQ engine in compaction config only supported with supervisor-based compaction on the Overlord.");
      } else {
        return current.withDatasourceConfig(newConfig);
      }
    };
    return updateConfigHelper(
        callable,
        AuthorizationUtils.buildAuditInfo(req)
    );
  }

  @GET
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatasourceCompactionConfig(@PathParam("dataSource") String dataSource)
  {
    final DruidCompactionConfig current = configManager.getCurrentCompactionConfig();
    final Optional<DataSourceCompactionConfig> config = current.findConfigForDatasource(dataSource);
    if (config.isPresent()) {
      return Response.ok().entity(config.get()).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
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
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    try {
      List<AuditEntry> auditEntries;
      if (theInterval == null && count != null) {
        auditEntries = auditManager.fetchAuditHistory(
            DruidCompactionConfig.CONFIG_KEY,
            DruidCompactionConfig.CONFIG_KEY,
            count
        );
      } else {
        auditEntries = auditManager.fetchAuditHistory(
            DruidCompactionConfig.CONFIG_KEY,
            DruidCompactionConfig.CONFIG_KEY,
            theInterval
        );
      }
      DataSourceCompactionConfigHistory history = new DataSourceCompactionConfigHistory(dataSource);
      for (AuditEntry audit : auditEntries) {
        DruidCompactionConfig compactionConfig = configManager.convertBytesToCompactionConfig(
            audit.getPayload().serialized().getBytes(StandardCharsets.UTF_8)
        );
        history.add(compactionConfig, audit.getAuditInfo(), audit.getAuditTime());
      }
      return Response.ok(history.getEntries()).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ServletResourceUtils.sanitizeException(e))
                     .build();
    }
  }

  @DELETE
  @Path("/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteCompactionConfig(
      @PathParam("dataSource") String dataSource,
      @Context HttpServletRequest req
  )
  {
    UnaryOperator<DruidCompactionConfig> callable = current -> {
      final Map<String, DataSourceCompactionConfig> configs = current.dataSourceToCompactionConfigMap();
      final DataSourceCompactionConfig config = configs.remove(dataSource);
      if (config == null) {
        throw NotFound.exception("datasource not found");
      }

      return current.withDatasourceConfigs(ImmutableList.copyOf(configs.values()));
    };
    return updateConfigHelper(callable, AuthorizationUtils.buildAuditInfo(req));
  }

  private Response updateConfigHelper(
      UnaryOperator<DruidCompactionConfig> configOperator,
      AuditInfo auditInfo
  )
  {
    int attemps = 0;
    SetResult setResult = null;
    try {
      while (attemps < MAX_UPDATE_RETRIES) {
        setResult = configManager.getAndUpdateCompactionConfig(configOperator, auditInfo);
        if (setResult.isOk() || !setResult.isRetryable()) {
          break;
        }
        attemps++;
        updateRetryDelay();
      }
    }
    catch (DruidException e) {
      return ServletResourceUtils.buildErrorResponseFrom(e);
    }
    catch (Exception e) {
      LOG.warn(e, "Update compaction config failed");
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                     .entity(ImmutableMap.of("error", createErrorMessage(e)))
                     .build();
    }

    if (setResult.isOk()) {
      return Response.ok().build();
    } else if (setResult.getException() instanceof NoSuchElementException) {
      LOG.warn(setResult.getException(), "Update compaction config failed");
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      LOG.warn(setResult.getException(), "Update compaction config failed");
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", createErrorMessage(setResult.getException())))
                     .build();
    }
  }

  private void updateRetryDelay()
  {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private String createErrorMessage(Exception e)
  {
    if (e.getMessage() == null) {
      return "Unknown Error";
    } else {
      return e.getMessage();
    }
  }
}
