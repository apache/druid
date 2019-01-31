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

package org.apache.druid.security.basic.escalator.endpoint;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.escalator.db.cache.BasicEscalatorCacheManager;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;
import org.apache.druid.server.security.AuthenticatorMapper;

import javax.ws.rs.core.Response;

public class DefaultBasicEscalatorResourceHandler implements BasicEscalatorResourceHandler
{
  private static final Logger LOG = new Logger(DefaultBasicEscalatorResourceHandler.class);
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  private final BasicEscalatorCacheManager cacheManager;

  @Inject
  public DefaultBasicEscalatorResourceHandler(
      BasicEscalatorCacheManager cacheManager,
      AuthenticatorMapper authenticatorMapper
  )
  {
    this.cacheManager = cacheManager;
  }

  @Override
  public Response refreshAll()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getEscalatorCredential()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response updateEscalatorCredential(BasicEscalatorCredential escalatorCredential)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedSerializedEscalatorCredential()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response escalatorCredentialUpdateListener(byte[] serializedEscalatorCredential)
  {
    cacheManager.handleEscalatorCredentialUpdate(serializedEscalatorCredential);
    return Response.ok().build();
  }

  @Override
  public Response getLoadStatus()
  {
    Boolean loadStatus = cacheManager.getEscalatorCredential() != null;
    return Response.ok(loadStatus).build();
  }
}
