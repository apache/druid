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
import com.google.inject.Singleton;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;

/**
 * This class is annotated {@link Singleton} rather than {@link org.apache.druid.guice.LazySingleton}, because it adds
 * a lifecycle handler in the constructor, that should happen before the lifecycle is started, i. e. eagerly during the
 * DI configuration phase.
 */
@Singleton
@Path("/selfDiscovered")
@ResourceFilters(StateResourceFilter.class)
public class SelfDiscoveryResource
{
  private boolean selfDiscovered = false;

  @Inject
  public SelfDiscoveryResource(
      @Self DruidNode thisDruidNode,
      @Self NodeType thisNodeType,
      DruidNodeDiscoveryProvider nodeDiscoveryProvider,
      Lifecycle lifecycle
  )
  {
    Lifecycle.Handler selfDiscoveryListenerRegistrator = new Lifecycle.Handler()
    {
      @Override
      public void start()
      {
        registerSelfDiscoveryListener(thisDruidNode, thisNodeType, nodeDiscoveryProvider);
      }

      @Override
      public void stop()
      {
        // do nothing
      }
    };
    // Using Lifecycle.Stage.LAST because DruidNodeDiscoveryProvider should be already started when
    // registerSelfDiscoveryListener() is called.
    lifecycle.addHandler(selfDiscoveryListenerRegistrator, Lifecycle.Stage.LAST);
  }

  private void registerSelfDiscoveryListener(
      DruidNode thisDruidNode,
      NodeType thisNodeType,
      DruidNodeDiscoveryProvider nodeDiscoveryProvider
  )
  {
    nodeDiscoveryProvider.getForNodeType(thisNodeType).registerListener(new DruidNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
      {
        if (selfDiscovered) {
          return;
        }
        for (DiscoveryDruidNode node : nodes) {
          if (node.getDruidNode().equals(thisDruidNode)) {
            selfDiscovered = true;
            break;
          }
        }
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
      {
        // do nothing
      }
    });
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSelfDiscovered()
  {
    return Response.ok(Collections.singletonMap("selfDiscovered", selfDiscovered)).build();
  }
}
