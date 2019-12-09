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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * This class is annotated {@link Singleton} rather than {@link org.apache.druid.guice.LazySingleton} because it adds
 * a lifecycle handler in the constructor. That should happen before the lifecycle is started, i. e. eagerly during the
 * DI configuration phase.
 */
@Singleton
@Path("/status/selfDiscovered")
public class SelfDiscoveryResource
{
  private final List<BooleanSupplier> selfDiscoveredRoles;

  @Inject
  public SelfDiscoveryResource(
      @Self DruidNode thisDruidNode,
      @Self Set<NodeRole> thisNodeRoles,
      DruidNodeDiscoveryProvider nodeDiscoveryProvider,
      Lifecycle lifecycle
  )
  {
    selfDiscoveredRoles = Lists.newArrayListWithExpectedSize(thisNodeRoles.size());
    thisNodeRoles.forEach(
        thisNodeRole -> {
          Lifecycle.Handler selfDiscoveryListenerRegistrator = new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              selfDiscoveredRoles.add(nodeDiscoveryProvider.getForNode(thisDruidNode, thisNodeRole));
            }

            @Override
            public void stop()
            {
              // do nothing
            }
          };
          // Using Lifecycle.Stage.SERVER because DruidNodeDiscoveryProvider should be already started when
          // selfDiscoveryListenerRegistrator.start() is called.
          lifecycle.addHandler(selfDiscoveryListenerRegistrator, Lifecycle.Stage.SERVER);
        }
    );
  }

  /** See the description of this endpoint in api-reference.md. */
  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getSelfDiscoveredStatus()
  {
    return Response.ok(Collections.singletonMap("selfDiscovered", isDiscoveredAllRoles())).build();
  }

  /** See the description of this endpoint in api-reference.md. */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getSelfDiscovered()
  {
    if (isDiscoveredAllRoles()) {
      return Response.ok().build();
    } else {
      return Response.status(HttpStatus.SERVICE_UNAVAILABLE_503).build();
    }
  }

  private boolean isDiscoveredAllRoles()
  {
    return selfDiscoveredRoles.stream().allMatch(BooleanSupplier::getAsBoolean);
  }
}
