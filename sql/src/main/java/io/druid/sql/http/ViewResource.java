/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataViewManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/v2/sql/view")
public class ViewResource {
    private static final Logger log = new Logger(ViewResource.class);

    private final MetadataViewManager metadataViewManager;

    @Inject
    public ViewResource(MetadataViewManager viewManager) {
        this.metadataViewManager = Preconditions.checkNotNull(viewManager, "viewManager");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response doPost(final ViewDefinition viewDefinition)
    {
        try {
            metadataViewManager.insertOrUpdate(viewDefinition.getName(), viewDefinition.getSql());
            return Response.ok(ImmutableMap.<String, Object>of("result", "ok")).build();
        } catch (Exception e) {
            log.warn(e, "Failed to handle view creation/update: %s", viewDefinition);
            return Response.serverError().entity(ImmutableMap.<String, Object>of("error", e.getMessage())).build();
        }
    }

    @GET
    @Path("/definitions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getViewDefinitions() {
        return Response.ok(metadataViewManager.getAll()).build();
    }

    @DELETE
    @Path("/{viewName}")
    public Response deleteView(@PathParam("viewName") String viewName) {
        try {
            metadataViewManager.remove(viewName);
            return Response.ok().build();
        } catch (Exception e) {
            return Response.serverError().entity(ImmutableMap.<String, Object>of("error", e.getMessage())).build();
        }
    }
}
