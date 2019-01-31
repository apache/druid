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

import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

import javax.ws.rs.core.Response;

/**
 * Handles authenticator-related API calls. Coordinator and non-coordinator methods are combined here because of an
 * inability to selectively inject jetty resources in configure(Binder binder) of the extension module based
 * on node type.
 */
public interface BasicEscalatorResourceHandler
{
  // coordinator methods
  Response getEscalatorCredential();

  Response updateEscalatorCredential(BasicEscalatorCredential escalatorCredential);

  Response getCachedSerializedEscalatorCredential();

  Response refreshAll();

  // non-coordinator methods
  Response escalatorCredentialUpdateListener(byte[] serializedEscalatorCredential);

  // common methods
  Response getLoadStatus();
}
