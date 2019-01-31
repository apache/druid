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

package org.apache.druid.security.authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import org.apache.druid.security.basic.authentication.entity.BasicAuthConfig;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class CoordinatorBasicAuthenticatorMetadataStorageUpdaterTest
{
  private static final String AUTHENTICATOR_NAME = "test";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private CoordinatorBasicAuthenticatorMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();
    updater = new CoordinatorBasicAuthenticatorMetadataStorageUpdater(
        new AuthenticatorMapper(
            ImmutableMap.of(
                "test",
                new BasicHTTPAuthenticator(
                    null,
                    "test",
                    "test",
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        objectMapper,
        new NoopBasicAuthenticatorCacheNotifier(),
        null
    );

    updater.start();
  }

  @After
  public void tearDown()
  {
    updater.stop();
  }

  @Test
  public void createUser()
  {
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "druid", new BasicAuthenticatorUser("druid", null)
    );
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

    updater.createUser(AUTHENTICATOR_NAME, "druid");
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

    Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // Validate cache user map methods
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));

    // create duplicate should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already exists.");
    updater.createUser(AUTHENTICATOR_NAME, "druid");
  }

  @Test
  public void deleteUser()
  {
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of();
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

    updater.createUser(AUTHENTICATOR_NAME, "druid");
    updater.deleteUser(AUTHENTICATOR_NAME, "druid");

    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

    Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // Validate cache user map methods
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));

    // delete non-existent user should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    updater.deleteUser(AUTHENTICATOR_NAME, "druid");
  }

  @Test
  public void setCredentials()
  {
    updater.createUser(AUTHENTICATOR_NAME, "druid");
    updater.setUserCredentials(AUTHENTICATOR_NAME, "druid", new BasicAuthenticatorCredentialUpdate("helloworld", null));

    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    BasicAuthenticatorCredentials credentials = userMap.get("druid").getCredentials();

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        credentials.getSalt(),
        credentials.getIterations()
    );

    Assert.assertArrayEquals(credentials.getHash(), recalculatedHash);

    // Validate cache user map methods
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "druid", new BasicAuthenticatorUser("druid", credentials)
    );
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));
  }

  @Test
  public void updateConfig()
  {
    BasicAuthConfig config = new BasicAuthConfig(
        "https://testUrl",
        "testUser",
        "testPassword",
        "testDn",
        "testUserSearch",
        "testUserAttribute",
        new String[]{"testGroupFilter"}
    );
    byte[] serializedConfig = BasicAuthUtils.serializeAuthenticatorConfig(objectMapper, config);

    updater.updateConfig(AUTHENTICATOR_NAME, config);

    Assert.assertArrayEquals(serializedConfig, updater.getCurrentConfigBytes(AUTHENTICATOR_NAME));

    BasicAuthConfig actualConfig = BasicAuthUtils.deserializeAuthenticatorConfig(
        objectMapper,
        updater.getCurrentConfigBytes(AUTHENTICATOR_NAME)
    );
    Assert.assertNotNull(actualConfig);
    Assert.assertEquals(config.getUrl(), actualConfig.getUrl());
    Assert.assertEquals(config.getBindUser(), actualConfig.getBindUser());
    Assert.assertEquals(config.getBindPassword(), actualConfig.getBindPassword());
    Assert.assertEquals(config.getBaseDn(), actualConfig.getBaseDn());
    Assert.assertEquals(config.getUserSearch(), actualConfig.getUserSearch());
    Assert.assertEquals(config.getUserAttribute(), actualConfig.getUserAttribute());
    Assert.assertArrayEquals(config.getGroupFilters(), actualConfig.getGroupFilters());

    // Validate cache config methods
    BasicAuthConfig cachedConfig = updater.getCachedConfig(AUTHENTICATOR_NAME);
    Assert.assertNotNull(actualConfig);
    Assert.assertEquals(config.getUrl(), cachedConfig.getUrl());
    Assert.assertEquals(config.getBindUser(), cachedConfig.getBindUser());
    Assert.assertEquals(config.getBindPassword(), cachedConfig.getBindPassword());
    Assert.assertEquals(config.getBaseDn(), cachedConfig.getBaseDn());
    Assert.assertEquals(config.getUserSearch(), cachedConfig.getUserSearch());
    Assert.assertEquals(config.getUserAttribute(), cachedConfig.getUserAttribute());
    Assert.assertArrayEquals(config.getGroupFilters(), cachedConfig.getGroupFilters());

    Assert.assertArrayEquals(serializedConfig, updater.getCachedSerializedConfig(AUTHENTICATOR_NAME));

    // update duplicate should not fail
    updater.updateConfig(AUTHENTICATOR_NAME, config);
  }

}
