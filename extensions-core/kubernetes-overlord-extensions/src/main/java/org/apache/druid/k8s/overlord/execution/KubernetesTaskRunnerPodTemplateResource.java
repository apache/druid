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

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.error.DruidException;
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.druid.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Resource that exposes the names of the pod templates currently configured for running peons in
 * Kubernetes.
 *
 * <p>Pod templates only exist for the pod template adapter ("customTemplateAdapter"). For any other
 * adapter this endpoint returns a 404.</p>
 */
@Path("/druid/indexer/v1/k8s/taskrunner/podTemplates")
public class KubernetesTaskRunnerPodTemplateResource
{
  private final TaskAdapter taskAdapter;

  @Inject
  public KubernetesTaskRunnerPodTemplateResource(final TaskAdapter taskAdapter)
  {
    this.taskAdapter = taskAdapter;
  }

  /**
   * Retrieves the names of the currently configured peon pod templates.
   *
   * @return a Response with the configured template names (200), or 404 when the configured adapter
   *         has no pod templates.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getPodTemplateNames()
  {
    if (!(taskAdapter instanceof PodTemplateTaskAdapter)) {
      return ServletResourceUtils.buildErrorResponseFrom(
          DruidException.forPersona(DruidException.Persona.OPERATOR)
                        .ofCategory(DruidException.Category.NOT_FOUND)
                        .build(
                            "Pod templates are only available when the k8s task adapter type is [%s]",
                            PodTemplateTaskAdapter.TYPE
                        )
      );
    }
    return Response.ok(((PodTemplateTaskAdapter) taskAdapter).getPodTemplateSelector().getPodTemplateNames()).build();
  }
}
