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

package org.apache.druid.query.lookup;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.security.ConfigResourceFilter;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.Optional;

@Path("/druid/v1/lookups/introspect")
@ResourceFilters(ConfigResourceFilter.class)
public class LookupIntrospectionResource
{
  private static final Logger LOGGER = new Logger(LookupIntrospectionResource.class);

  private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;

  @Inject
  public LookupIntrospectionResource(
      @Context LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider
  )
  {
    this.lookupExtractorFactoryContainerProvider = lookupExtractorFactoryContainerProvider;
  }

  @Path("/{lookupId}")
  public Object introspectLookup(@PathParam("lookupId") final String lookupId)
  {
    final Optional<LookupExtractorFactoryContainer> maybeContainer =
        lookupExtractorFactoryContainerProvider.get(lookupId);

    if (!maybeContainer.isPresent()) {
      LOGGER.error("trying to introspect non existing lookup [%s]", lookupId);
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    final LookupExtractorFactoryContainer container = maybeContainer.get();
    LookupIntrospectHandler introspectHandler = container.getLookupExtractorFactory().getIntrospectHandler();
    if (introspectHandler != null) {
      return introspectHandler;
    } else {
      LOGGER.warn(
          "Trying to introspect lookup [%s] of type [%s] but implementation doesn't provide resource",
          lookupId,
          container.getLookupExtractorFactory().get().getClass()
      );
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
