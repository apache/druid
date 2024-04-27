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

package org.apache.druid.catalog.http;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.catalog.sync.CatalogUpdateListener;
import org.apache.druid.catalog.sync.UpdateEvent;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Resource on the Broker to listen to catalog update events from the
 * Coordinator. Since this is an internal API, it supports the efficient
 * Smile encoding as well as the JSON encoding. (JSON is more convenient
 * for debugging as it can be easily created or interpreted.)
 */
@Path(CatalogListenerResource.BASE_URL)
public class CatalogListenerResource
{
  public static final String BASE_URL = "/druid/broker/v1/catalog";
  public static final String SYNC_URL = "/sync";

  private final CatalogUpdateListener listener;

  @Inject
  public CatalogListenerResource(
      final CatalogUpdateListener listener
  )
  {
    this.listener = listener;
  }

  /**
   * Event sent from the Coordinator to indicate that a table has changed
   * (or been deleted.)
   */
  @POST
  @Path(SYNC_URL)
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @ResourceFilters(ConfigResourceFilter.class)
  public Response syncTable(final UpdateEvent event)
  {
    listener.updated(event);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Path("flush")
  @ResourceFilters(ConfigResourceFilter.class)
  public Response flush()
  {
    listener.flush();
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Path("resync")
  @ResourceFilters(ConfigResourceFilter.class)
  public Response resync()
  {
    listener.resync();
    return Response.status(Response.Status.ACCEPTED).build();
  }
}
