/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.http;

import io.druid.common.config.JacksonConfigManager;
import io.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 */
@Path("/druid/coordinator/v1/config")
public class CoordinatorDynamicConfigsResource
{
  private final JacksonConfigManager manager;

  @Inject
  public CoordinatorDynamicConfigsResource(
      JacksonConfigManager manager
  )
  {
    this.manager = manager;
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

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response setDynamicConfigs(final CoordinatorDynamicConfig dynamicConfig)
  {
    if (!manager.set(CoordinatorDynamicConfig.CONFIG_KEY, dynamicConfig)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

}
