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

package org.apache.druid.security.pac4j;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.logger.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * Fixed Callback endpoint used after successful login with Identity Provider e.g. OAuth server.
 * See https://www.pac4j.org/blog/understanding-the-callback-endpoint.html
 */
@Path(Pac4jCallbackResource.SELF_URL)
@LazySingleton
public class Pac4jCallbackResource
{
  public static final String SELF_URL = "/druid-ext/druid-pac4j/callback";

  private static final Logger LOGGER = new Logger(Pac4jCallbackResource.class);

  @Inject
  public Pac4jCallbackResource()
  {
  }

  @GET
  public Response callback()
  {
    LOGGER.error(
        new RuntimeException(),
        "This endpoint is to be handled by the pac4j filter to redirect users, request should never reach here."
    );
    return Response.serverError().build();
  }
}

