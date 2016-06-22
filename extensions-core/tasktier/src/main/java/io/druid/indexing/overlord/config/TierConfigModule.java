/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.utils.ServletResourceUtils;
import io.druid.guice.JacksonConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.indexing.overlord.TierTaskDiscovery;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;
import io.druid.initialization.DruidModule;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TierConfigModule implements DruidModule
{
  static final String ROUTE_CONFIG_KEY = "druid.tier.route.config";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("TierConfigModule").registerSubtypes(
            TierRouteConfig.class,
            TierTaskRunnerFactory.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.indexer.runner", TierLocalTaskRunnerConfig.class);
    JsonConfigProvider.bind(binder, "druid.zk.paths", TierForkZkConfig.class);
    Jerseys.addResource(binder, TierConfigResource.class);
    binder.bind(TierTaskDiscovery.class).in(LazySingleton.class);
    JacksonConfigProvider.bind(binder, ROUTE_CONFIG_KEY, TierRouteConfig.class, null);
  }
}

@Path("/druid/tier/v1/config")
class TierConfigResource
{
  private static final Logger LOG = new Logger(TierConfigResource.class);
  private final JacksonConfigManager configManager;
  private final AtomicReference<TierRouteConfig> routeConfigRef;

  @Inject
  public TierConfigResource(
      final JacksonConfigManager configManager
  )
  {
    this.configManager = configManager;
    routeConfigRef = configManager.watch(TierConfigModule.ROUTE_CONFIG_KEY, TierRouteConfig.class, null);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateConfig(
      final TierRouteConfig tierRouteConfig,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context final HttpServletRequest req
  )
  {
    if (!configManager.set(
        TierConfigModule.ROUTE_CONFIG_KEY,
        tierRouteConfig,
        new AuditInfo(author, comment, req.getRemoteHost())
    )) {
      LOG.debug("Unable to set %s from [%s]", tierRouteConfig, req.getRemoteHost());
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    LOG.info("Updated tier route config per request from %s@%s: %s", author, req.getRemoteHost(), comment);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfig(
      @Context final HttpServletRequest req
  )
  {
    final TierRouteConfig routeConfig = routeConfigRef.get();
    if (routeConfig == null) {
      LOG.debug("Requested config from [%s] but no config is set.", req.getRemoteHost());
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(ServletResourceUtils.sanitizeException(new IllegalArgumentException("No config set")))
                     .build();
    }
    LOG.debug("Returning config to [%s]", req.getRemoteHost());
    return Response.ok(routeConfig).build();
  }
}
