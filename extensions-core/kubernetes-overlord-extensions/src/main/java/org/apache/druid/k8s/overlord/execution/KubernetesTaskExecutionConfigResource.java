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

package org.apache.druid.k8s.overlord.execution;

import com.google.common.collect.ImmutableMap;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resource that manages Kubernetes-specific execution configurations for running tasks.
 *
 * <p>This class handles the CRUD operations for execution configurations and provides
 * endpoints to update, retrieve, and manage the history of these configurations.</p>
 */
@Path("/druid/indexer/v1/k8s/taskrunner/executionconfig")
public class KubernetesTaskExecutionConfigResource
{
  private static final Logger log = new Logger(KubernetesTaskExecutionConfigResource.class);
  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private AtomicReference<KubernetesTaskRunnerDynamicConfig> dynamicConfigRef = null;

  @Inject
  public KubernetesTaskExecutionConfigResource(
      final JacksonConfigManager configManager,
      final AuditManager auditManager
  )
  {
    this.configManager = configManager;
    this.auditManager = auditManager;
  }

  /**
   * Updates the Kubernetes execution configuration.
   *
   * @param dynamicConfig the new execution configuration to set
   * @param req             the HTTP servlet request providing context for audit information
   * @return a response indicating the success or failure of the update operation
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response setExecutionConfig(
      final KubernetesTaskRunnerDynamicConfig dynamicConfig,
      @Context final HttpServletRequest req
  )
  {
    final ConfigManager.SetResult setResult = configManager.set(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        dynamicConfig,
        AuthorizationUtils.buildAuditInfo(req)
    );
    if (setResult.isOk()) {
      log.info("Updating K8s execution configs: %s", dynamicConfig);

      return Response.ok().build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
  }

  /**
   * Retrieves the history of changes to the Kubernetes execution configuration.
   *
   * @param interval the time interval for fetching historical data (optional)
   * @param count    the maximum number of historical entries to fetch (optional)
   * @return a response containing a list of audit entries or an error message
   */
  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getExecutionConfigHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    if (theInterval == null && count != null) {
      try {
        List<AuditEntry> executionEntryList = auditManager.fetchAuditHistory(
            KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
            KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
            count
        );
        return Response.ok(executionEntryList).build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
    }
    List<AuditEntry> executionEntryList = auditManager.fetchAuditHistory(
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        KubernetesTaskRunnerDynamicConfig.CONFIG_KEY,
        theInterval
    );
    return Response.ok(executionEntryList).build();
  }

  /**
   * Retrieves the current execution configuration for tasks running in Kubernetes.
   *
   * @return a Response object containing the current execution configuration in JSON format.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getExecutionConfig()
  {
    if (dynamicConfigRef == null) {
      dynamicConfigRef = configManager.watch(KubernetesTaskRunnerDynamicConfig.CONFIG_KEY, KubernetesTaskRunnerDynamicConfig.class);
    }

    return Response.ok(dynamicConfigRef.get()).build();
  }
}
