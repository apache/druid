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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.escalator.db.updater.BasicEscalatorMetadataStorageUpdater;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

import javax.ws.rs.core.Response;

public class CoordinatorBasicEscalatorResourceHandler implements BasicEscalatorResourceHandler
{
  private final BasicEscalatorMetadataStorageUpdater storageUpdater;
  private final ObjectMapper objectMapper;

  @Inject
  public CoordinatorBasicEscalatorResourceHandler(
      BasicEscalatorMetadataStorageUpdater storageUpdater,
      @Smile ObjectMapper objectMapper
  )
  {
    this.storageUpdater = storageUpdater;
    this.objectMapper = objectMapper;
  }

  @Override
  public Response refreshAll()
  {
    storageUpdater.refreshAllNotification();
    return Response.ok().build();
  }

  @Override
  public Response getEscalatorCredential()
  {
    BasicEscalatorCredential escalatorCredential = BasicAuthUtils.deserializeEscalatorCredential(
        objectMapper,
        storageUpdater.getCurrentEscalatorCredentialBytes()
    );
    if (escalatorCredential == null) {
      return Response.status(Response.Status.NOT_FOUND)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         "Escalator credential does not exist."
                     ))
                     .build();
    } else {
      BasicEscalatorCredential maskedEscalatorCredential = new BasicEscalatorCredential(
          escalatorCredential.getUsername(),
          "..."
      );

      return Response.ok(maskedEscalatorCredential).build();
    }
  }

  @Override
  public Response updateEscalatorCredential(BasicEscalatorCredential escalatorCredential)
  {
    try {
      storageUpdater.updateEscalatorCredential(escalatorCredential);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response getCachedSerializedEscalatorCredential()
  {
    return Response.ok(storageUpdater.getCachedSerializedEscalatorCredential()).build();
  }

  @Override
  public Response escalatorCredentialUpdateListener(byte[] serializedEscalatorCredential)
  {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Override
  public Response getLoadStatus()
  {
    Boolean loadStatus = storageUpdater.getCachedEscalatorCredential() != null;
    return Response.ok(loadStatus).build();
  }

  private static Response makeResponseForBasicSecurityDBResourceException(BasicSecurityDBResourceException bsre)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error", bsre.getMessage()
                   ))
                   .build();
  }
}
