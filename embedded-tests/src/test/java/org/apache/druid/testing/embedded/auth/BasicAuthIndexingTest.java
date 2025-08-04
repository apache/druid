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

package org.apache.druid.testing.embedded.auth;

import org.apache.druid.security.basic.BasicSecurityDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexing.IndexTaskTest;

public class BasicAuthIndexingTest extends IndexTaskTest
{
  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return super
        .createCluster()
        .addExtension(BasicSecurityDruidModule.class)
        .addCommonProperty("druid.auth.authenticatorChain", "[\"basic\"]")
        .addCommonProperty("druid.auth.authenticator.basic.type", "basic")
        .addCommonProperty("druid.auth.authenticator.basic.initialAdminPassword", "priest")
        .addCommonProperty("druid.auth.authenticator.basic.initialInternalClientPassword", "warlock")
        .addCommonProperty("druid.auth.authenticator.basic.authorizerName", "basic")
        .addCommonProperty("druid.auth.authorizers", "[\"basic\"]")
        .addCommonProperty("druid.auth.authorizer.basic.type", "basic")
        .addCommonProperty("druid.escalator.type", "basic")
        .addCommonProperty("druid.escalator.internalClientPassword", "warlock")
        .addCommonProperty("druid.escalator.internalClientUsername", "druid_system")
        .addCommonProperty("druid.escalator.authorizerName", "basic");
  }
}
