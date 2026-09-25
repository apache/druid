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
import org.apache.druid.metadata.JUnit5TestDerbyConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.security.basic.authentication.validator.PasswordHashGenerator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorBasicAuthenticatorMetadataStorageUpdaterTest
{
  private static final String AUTHENTICATOR_NAME = "test";

  @RegisterExtension
  public static final JUnit5TestDerbyConnector DERBY_CONNECTOR_RULE = new JUnit5TestDerbyConnector();

  private CoordinatorBasicAuthenticatorMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp()
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    TestDerbyConnector connector = DERBY_CONNECTOR_RULE.getConnector();
    MetadataStorageTablesConfig tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
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

  @AfterEach
  public void tearDown()
  {
    updater.stop();
  }

  @Test
  public void createUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
          "druid", new BasicAuthenticatorUser("druid", null)
      );
      byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

      updater.createUser(AUTHENTICATOR_NAME, "druid");
      Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

      Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
          objectMapper,
          updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
      );
      Assertions.assertEquals(expectedUserMap, actualUserMap);

      // Validate cache user map methods
      Assertions.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
      Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));
      updater.createUser(AUTHENTICATOR_NAME, "druid");
    });
    assertTrue(exception.getMessage().contains("User [druid] already exists."));
  }

  @Test
  public void deleteUser()
  {
    Throwable exception = assertThrows(BasicSecurityDBResourceException.class, () -> {
      Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of();
      byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

      updater.createUser(AUTHENTICATOR_NAME, "druid");
      updater.deleteUser(AUTHENTICATOR_NAME, "druid");

      Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

      Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
          objectMapper,
          updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
      );
      Assertions.assertEquals(expectedUserMap, actualUserMap);

      // Validate cache user map methods
      Assertions.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
      Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));
      updater.deleteUser(AUTHENTICATOR_NAME, "druid");
    });
    assertTrue(exception.getMessage().contains("User [druid] does not exist."));
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

    byte[] recalculatedHash = PasswordHashGenerator.computePasswordHash(
        "helloworld".toCharArray(),
        credentials.getSalt(),
        credentials.getIterations()
    );

    Assertions.assertArrayEquals(credentials.getHash(), recalculatedHash);

    // Validate cache user map methods
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "druid", new BasicAuthenticatorUser("druid", credentials)
    );
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);
    Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    Assertions.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assertions.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));
  }
}
