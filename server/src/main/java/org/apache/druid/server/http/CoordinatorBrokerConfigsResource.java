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
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.joda.time.Interval;

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
import java.util.concurrent.atomic.AtomicReference;

@Path("/druid/coordinator/v1/broker/config")
@ResourceFilters(ConfigResourceFilter.class)
public class CoordinatorBrokerConfigsResource
{
  private static final Logger log = new Logger(CoordinatorBrokerConfigsResource.class);

  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private final AtomicReference<BrokerDynamicConfig> currentConfig;
  private final BrokerDynamicConfigSyncer brokerDynamicConfigSyncer;

  @Inject
  public CoordinatorBrokerConfigsResource(
      JacksonConfigManager configManager,
      AuditManager auditManager,
      BrokerDynamicConfigSyncer brokerDynamicConfigSyncer
  )
  {
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.currentConfig = configManager.watch(
        BrokerDynamicConfig.CONFIG_KEY,
        BrokerDynamicConfig.class,
        BrokerDynamicConfig.builder().build()
    );
    this.brokerDynamicConfigSyncer = brokerDynamicConfigSyncer;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBrokerDynamicConfig()
  {
    return Response.ok(currentConfig.get()).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setBrokerDynamicConfig(
      final BrokerDynamicConfig.Builder configBuilder,
      @Context HttpServletRequest req
  )
  {
    try {
      BrokerDynamicConfig current = currentConfig.get();
      BrokerDynamicConfig newConfig = configBuilder.build(current);

      final SetResult setResult = configManager.set(
          BrokerDynamicConfig.CONFIG_KEY,
          newConfig,
          AuthorizationUtils.buildAuditInfo(req)
      );

      if (setResult.isOk()) {
        brokerDynamicConfigSyncer.queueBroadcastConfigToBrokers();
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
  public Response getBrokerDynamicConfigHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    if (theInterval == null && count != null) {
      try {
        return Response.ok(
            auditManager.fetchAuditHistory(
                BrokerDynamicConfig.CONFIG_KEY,
                BrokerDynamicConfig.CONFIG_KEY,
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
            BrokerDynamicConfig.CONFIG_KEY,
            BrokerDynamicConfig.CONFIG_KEY,
            theInterval
        )
    ).build();
  }

}
