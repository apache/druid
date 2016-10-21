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

package io.druid.query.lookup;

import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/druid/v1/lookups/introspect")
public class LookupIntrospectionResource
{
  private static final Logger LOGGER = new Logger(LookupIntrospectionResource.class);

  private final LookupReferencesManager lookupReferencesManager;

  @Inject
  public LookupIntrospectionResource(@Context LookupReferencesManager lookupReferencesManager)
  {
    this.lookupReferencesManager = lookupReferencesManager;
  }

  @Path("/{lookupId}")
  public Object introspectLookup(@PathParam("lookupId") final String lookupId)
  {
    final LookupExtractorFactory lookupExtractorFactory = lookupReferencesManager.get(lookupId);
    if (lookupExtractorFactory == null) {
      LOGGER.error("trying to introspect non existing lookup [%s]", lookupId);
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    LookupIntrospectHandler introspectHandler = lookupExtractorFactory.getIntrospectHandler();
    if (introspectHandler != null) {
      return introspectHandler;
    } else {
      LOGGER.warn(
          "Trying to introspect lookup [%s] of type [%s] but implementation doesn't provide resource",
          lookupId,
          lookupExtractorFactory.get().getClass()
      );
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
