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

package org.apache.druid.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.DBRoleProvider;
import org.apache.druid.security.basic.authorization.LDAPRoleProvider;
import org.apache.druid.security.basic.authorization.db.cache.MetadataStoragePollingBasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class BasicRoleBasedAuthorizerTest
{
  private static final String DB_AUTHORIZER_NAME = "db";
  private static final String LDAP_AUTHORIZER_NAME = "ldap";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private BasicRoleBasedAuthorizer authorizer;
  private BasicRoleBasedAuthorizer ldapAuthorizer;

  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;

  @Before
  public void setUp()
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
            ImmutableMap.of(
                DB_AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    DB_AUTHORIZER_NAME,
                    null,
                    null,
                    null, null,
                    null,
                    new DBRoleProvider(null)
                ),
                LDAP_AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    LDAP_AUTHORIZER_NAME,
                    null,
                    null,
                    null, null,
                    null,
                    new LDAPRoleProvider(null)
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    updater.start();

    authorizer = new BasicRoleBasedAuthorizer(
        null,
        DB_AUTHORIZER_NAME,
        null,
        null,
        null, null,
        null,
        new DBRoleProvider(new MetadataStoragePollingBasicAuthorizerCacheManager(updater))
    );

    ldapAuthorizer = new BasicRoleBasedAuthorizer(
        null,
        LDAP_AUTHORIZER_NAME,
        null,
        null,
        null, null,
        null,
        new LDAPRoleProvider(new MetadataStoragePollingBasicAuthorizerCacheManager(updater))
    );
  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void testAuth()
  {
    updater.createUser(DB_AUTHORIZER_NAME, "druid");
    updater.createRole(DB_AUTHORIZER_NAME, "druidRole");
    updater.assignUserRole(DB_AUTHORIZER_NAME, "druid", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(DB_AUTHORIZER_NAME, "druidRole", permissions);

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);

    Access access = authorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = authorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthGroupMapping() throws InvalidNameException
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("druidGroupMapping", "CN=test", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "druidGroupMapping", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "druidRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=test")));

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthGroupMappingPatternRightMask() throws InvalidNameException
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("druidGroupMapping", "CN=test,*", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "druidGroupMapping", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "druidRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();

    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=test,ou=groupings,dc=corp")));
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=test")));
    authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=druid,CN=test")));
    authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthGroupMappingPatternLeftMask() throws InvalidNameException
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("druidGroupMapping", "*,CN=test", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "druidGroupMapping", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "druidRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();

    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=druid,CN=test")));
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=test")));
    authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.GROUPS_CONTEXT_KEY, Collections.singleton(new LdapName("CN=test,CN=druid")));
    authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthMissingGroupMapping() throws InvalidNameException
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("druidGroupMapping", "CN=test", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "druidRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "druidGroupMapping", "druidRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "druidRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();
    contexMap.put(
        BasicAuthUtils.GROUPS_CONTEXT_KEY,
        new HashSet<>(Arrays.asList(new LdapName("CN=unknown"), new LdapName("CN=test,ou=groupings,dc=corp")))
    );

    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }
}
