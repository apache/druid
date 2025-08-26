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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.security.basic.BasicSecurityDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

/**
 * Resource to enable the basic auth extension in embedded tests.
 */
public class EmbeddedBasicAuthResource implements EmbeddedResource
{
  public static final String ADMIN_PASSWORD = "priest";
  public static final String SYSTEM_PASSWORD = "warlock";
  public static final String SYSTEM_USER = "druid_system";

  private static final String AUTHORIZER_NAME = "basic";
  private static final String AUTHENTICATOR_NAME = "basic";

  @Override
  public void start()
  {
    // Do nothing
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster
        .addExtension(BasicSecurityDruidModule.class)
        .addCommonProperty("druid.auth.authenticatorChain", StringUtils.format("[\"%s\"]", AUTHENTICATOR_NAME))
        .addCommonProperty(authenticatorProp("type"), "basic")
        .addCommonProperty(authenticatorProp("initialAdminPassword"), ADMIN_PASSWORD)
        .addCommonProperty(authenticatorProp("initialInternalClientPassword"), SYSTEM_PASSWORD)
        .addCommonProperty(authenticatorProp("authorizerName"), AUTHORIZER_NAME)
        .addCommonProperty("druid.auth.authorizers", StringUtils.format("[\"%s\"]", AUTHORIZER_NAME))
        .addCommonProperty(authorizerProp("type"), "basic")
        .addCommonProperty(escalatorProp("type"), "basic")
        .addCommonProperty(escalatorProp("internalClientPassword"), SYSTEM_PASSWORD)
        .addCommonProperty(escalatorProp("internalClientUsername"), SYSTEM_USER)
        .addCommonProperty(escalatorProp("authorizerName"), AUTHORIZER_NAME);
  }

  @Override
  public void stop()
  {
    // Do nothing
  }
  
  private String authenticatorProp(String name)
  {
    return StringUtils.format("druid.auth.authenticator.%s.%s", AUTHENTICATOR_NAME, name);
  }
  
  private String authorizerProp(String name)
  {
    return StringUtils.format("druid.auth.authorizer.%s.%s", AUTHORIZER_NAME, name);
  }
  
  private String escalatorProp(String name)
  {
    return StringUtils.format("druid.escalator.%s", name);
  }
}
