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

package org.apache.druid.server;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid-internal/v1/config")
public class BrokerDynamicConfigResource
{
  private final BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;

  @Inject
  public BrokerDynamicConfigResource(BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig)
  {
    this.brokerViewOfCoordinatorConfig = brokerViewOfCoordinatorConfig;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  @Path("/coordinator")
  public Response getDynamicConfig()
  {
    return Response.ok(brokerViewOfCoordinatorConfig.getDynamicConfig()).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  @Path("/coordinator")
  public Response setDynamicConfig(final CoordinatorDynamicConfig dynamicConfig)
  {
    brokerViewOfCoordinatorConfig.setDynamicConfig(dynamicConfig);
    return Response.ok().build();
  }
}
