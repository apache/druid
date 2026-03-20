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
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

/**
 * Resource to run an LDAP test container and add required properties to the
 * cluster for LDAP authentication.
 */
public class LdapAuthResource extends TestcontainerResource<GenericContainer<?>>
{
  private static final String IMAGE_NAME = "osixia/openldap:1.5.0";

  public static final String ADMIN_PASSWORD = "priest";
  public static final String SYSTEM_PASSWORD = "warlock";
  public static final String SYSTEM_USER = "druid_system";

  private static final String AUTHORIZER_NAME = "ldapauth";
  private static final String AUTHENTICATOR_NAME = "ldap";

  @Override
  protected GenericContainer<?> createContainer()
  {
    GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME))
        .withFileSystemBind(
            Resources.getFileForResource("ldap-configs").getAbsolutePath(),
            "/container/service/slapd/assets/config/bootstrap/ldif/custom",
            BindMode.READ_WRITE
        )
        .withExposedPorts(389, 636)
        .withCommand("--copy-service");

    container.setPortBindings(List.of("8389:389", "8636:636"));

    return container;
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster
        .addExtensions(BasicSecurityDruidModule.class)
        .addCommonProperty(authenticatorProp("authorizerName"), AUTHORIZER_NAME)
        .addCommonProperty(authenticatorProp("initialAdminPassword"), ADMIN_PASSWORD)
        .addCommonProperty(authenticatorProp("initialInternalClientPassword"), SYSTEM_PASSWORD)
        .addCommonProperty(authenticatorProp("type"), "basic")
        .addCommonProperty(authenticatorProp("credentialsValidator.type"), "ldap")
        .addCommonProperty(authenticatorProp("credentialsValidator.url"), "ldap://localhost:8389")
        .addCommonProperty(authenticatorProp("credentialsValidator.bindUser"), "cn=admin,dc=example,dc=org")
        .addCommonProperty(authenticatorProp("credentialsValidator.bindPassword"), "admin")
        .addCommonProperty(authenticatorProp("credentialsValidator.baseDn"), "ou=Users,dc=example,dc=org")
        .addCommonProperty(
            authenticatorProp("credentialsValidator.userSearch"),
            "(&(uid=%s)(objectClass=inetOrgPerson))"
        )
        .addCommonProperty(authenticatorProp("credentialsValidator.userAttribute"), "uid")
        .addCommonProperty("druid.auth.authenticatorChain", "[\"ldap\"]")
        .addCommonProperty(authorizerProp("type"), "basic")
        .addCommonProperty(authorizerProp("initialAdminUser"), "admin")
        .addCommonProperty(authorizerProp("initialAdminRole"), "admin")
        .addCommonProperty(authorizerProp("roleProvider.type"), "ldap")
        .addCommonProperty("druid.auth.authorizers", StringUtils.format("[\"%s\"]", AUTHORIZER_NAME))
        .addCommonProperty(escalatorProp("type"), "basic")
        .addCommonProperty(escalatorProp("internalClientPassword"), SYSTEM_PASSWORD)
        .addCommonProperty(escalatorProp("internalClientUsername"), SYSTEM_USER)
        .addCommonProperty(escalatorProp("authorizerName"), AUTHORIZER_NAME);
  }

  private String escalatorProp(String name)
  {
    return StringUtils.format("druid.escalator.%s", name);
  }

  private String authorizerProp(String name)
  {
    return StringUtils.format("druid.auth.authorizer.%s.%s", AUTHORIZER_NAME, name);
  }

  private String authenticatorProp(String name)
  {
    return StringUtils.format("druid.auth.authenticator.%s.%s", AUTHENTICATOR_NAME, name);
  }
}
