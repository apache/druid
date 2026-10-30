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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.JUnit5TestDerbyConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import org.apache.druid.security.basic.authentication.endpoint.BasicAuthenticatorResource;
import org.apache.druid.security.basic.authentication.endpoint.CoordinatorBasicAuthenticatorResourceHandler;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.security.basic.authentication.validator.PasswordHashGenerator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@ExtendWith(MockitoExtension.class)
public class CoordinatorBasicAuthenticatorResourceTest
{
  private static final String AUTHENTICATOR_NAME = "test";
  private static final String AUTHENTICATOR_NAME2 = "test2";
  private static final String AUTHENTICATOR_NAME_LDAP = "testLdap";

  @RegisterExtension
  public static final JUnit5TestDerbyConnector DERBY_CONNECTOR_RULE = new JUnit5TestDerbyConnector();

  @Mock
  private AuthValidator authValidator;

  @Mock
  private AuditManager auditManager;
  private BasicAuthenticatorResource resource;
  private CoordinatorBasicAuthenticatorMetadataStorageUpdater storageUpdater;
  private HttpServletRequest req;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp()
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").anyTimes();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("id", "authorizer", "authBy", Collections.emptyMap())
    ).anyTimes();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").anyTimes();
    EasyMock.replay(req);

    objectMapper = new ObjectMapper(new SmileFactory());
    TestDerbyConnector connector = DERBY_CONNECTOR_RULE.getConnector();
    MetadataStorageTablesConfig tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

    AuthenticatorMapper authenticatorMapper = new AuthenticatorMapper(
        ImmutableMap.of(
            AUTHENTICATOR_NAME,
            new BasicHTTPAuthenticator(
                null,
                AUTHENTICATOR_NAME,
                null,
                new DefaultPasswordProvider("druid"),
                new DefaultPasswordProvider("druid"),
                null,
                null,
                null,
                false,
                null
            ),
            AUTHENTICATOR_NAME2,
            new BasicHTTPAuthenticator(
                null,
                AUTHENTICATOR_NAME2,
                null,
                new DefaultPasswordProvider("druid"),
                new DefaultPasswordProvider("druid"),
                null,
                null,
                null,
                false,
                null
            ),
            AUTHENTICATOR_NAME_LDAP,
            new BasicHTTPAuthenticator(
                null,
                AUTHENTICATOR_NAME2,
                null,
                new DefaultPasswordProvider("druid"),
                new DefaultPasswordProvider("druid"),
                null,
                null,
                null,
                false,
                null
            )
        )
    );

    storageUpdater = new CoordinatorBasicAuthenticatorMetadataStorageUpdater(
        authenticatorMapper,
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        objectMapper,
        new NoopBasicAuthenticatorCacheNotifier(),
        null
    );

    resource = new BasicAuthenticatorResource(
        new CoordinatorBasicAuthenticatorResourceHandler(
            storageUpdater,
            authenticatorMapper,
            objectMapper
        ),
        authValidator,
        auditManager
    );

    storageUpdater.start();
  }

  @AfterEach
  public void tearDown()
  {
    storageUpdater.stop();
    if (req != null) {
      EasyMock.verify(req);
    }
  }

  @Test
  public void testInvalidAuthenticator()
  {
    Response response = resource.getAllUsers(mockHttpRequestNoAudit(), "invalidName");
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(
        errorMapWithMsg("Basic authenticator with name [invalidName] does not exist."),
        response.getEntity()
    );
  }

  @Test
  public void testGetAllUsers()
  {
    Response response = resource.getAllUsers(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME), response.getEntity());

    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid2");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid3");

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    response = resource.getAllUsers(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(expectedUsers, response.getEntity());

    // Verify cached user map is also getting updated
    response = resource.getCachedSerializedUserMap(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof byte[]);
    Map<String, BasicAuthenticatorUser> cachedUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(objectMapper, (byte[]) response.getEntity());
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.INTERNAL_USER_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get("druid"));
    Assertions.assertEquals(cachedUserMap.get("druid").getName(), "druid");
    Assertions.assertNotNull(cachedUserMap.get("druid2"));
    Assertions.assertEquals(cachedUserMap.get("druid2").getName(), "druid2");
    Assertions.assertNotNull(cachedUserMap.get("druid3"));
    Assertions.assertEquals(cachedUserMap.get("druid3").getName(), "druid3");
  }

  @Test
  public void testGetAllUsersSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME), response.getEntity());

    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid2");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid3");

    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME2, "druid4");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME2, "druid5");
    resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME2, "druid6");

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    Set<String> expectedUsers2 = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid4",
        "druid5",
        "druid6"
    );

    response = resource.getAllUsers(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(expectedUsers, response.getEntity());

    // Verify cached user map for AUTHENTICATOR_NAME authenticator is also getting updated
    response = resource.getCachedSerializedUserMap(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof byte[]);

    Map<String, BasicAuthenticatorUser> cachedUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(objectMapper, (byte[]) response.getEntity());
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.INTERNAL_USER_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get("druid"));
    Assertions.assertEquals(cachedUserMap.get("druid").getName(), "druid");
    Assertions.assertNotNull(cachedUserMap.get("druid2"));
    Assertions.assertEquals(cachedUserMap.get("druid2").getName(), "druid2");
    Assertions.assertNotNull(cachedUserMap.get("druid3"));
    Assertions.assertEquals(cachedUserMap.get("druid3").getName(), "druid3");

    response = resource.getAllUsers(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME2);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(expectedUsers2, response.getEntity());

    // Verify cached user map for each AUTHENTICATOR_NAME2 is also getting updated
    response = resource.getCachedSerializedUserMap(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME2);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof byte[]);

    cachedUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(objectMapper, (byte[]) response.getEntity());
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get(BasicAuthUtils.INTERNAL_USER_NAME));
    Assertions.assertEquals(cachedUserMap.get(BasicAuthUtils.ADMIN_NAME).getName(), BasicAuthUtils.ADMIN_NAME);
    Assertions.assertNotNull(cachedUserMap.get("druid4"));
    Assertions.assertEquals(cachedUserMap.get("druid4").getName(), "druid4");
    Assertions.assertNotNull(cachedUserMap.get("druid5"));
    Assertions.assertEquals(cachedUserMap.get("druid5").getName(), "druid5");
    Assertions.assertNotNull(cachedUserMap.get("druid6"));
    Assertions.assertEquals(cachedUserMap.get("druid6").getName(), "druid6");
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());

    response = resource.getUser(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());
    BasicAuthenticatorUser expectedUser = new BasicAuthenticatorUser("druid", null);
    Assertions.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());

    response = resource.getCachedSerializedUserMap(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof byte[]);
    Map<String, BasicAuthenticatorUser> cachedUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(objectMapper, (byte[]) response.getEntity());
    Assertions.assertNotNull(cachedUserMap);
    Assertions.assertNull(cachedUserMap.get("druid"));

    response = resource.deleteUser(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.getUser(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testUserCredentials()
  {
    Response response = resource.createUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());

    response = resource.updateUserCredentials(
        mockHttpRequest(),
        AUTHENTICATOR_NAME,
        "druid",
        new BasicAuthenticatorCredentialUpdate("helloworld", null)
    );
    Assertions.assertEquals(200, response.getStatus());

    response = resource.getUser(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());
    BasicAuthenticatorUser actualUser = (BasicAuthenticatorUser) response.getEntity();
    Assertions.assertEquals("druid", actualUser.getName());
    BasicAuthenticatorCredentials credentials = actualUser.getCredentials();

    byte[] salt = credentials.getSalt();
    byte[] hash = credentials.getHash();
    int iterations = credentials.getIterations();
    Assertions.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assertions.assertEquals(PasswordHashGenerator.KEY_LENGTH / 8, hash.length);
    Assertions.assertEquals(BasicAuthUtils.DEFAULT_KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = PasswordHashGenerator.computePasswordHash(
        "helloworld".toCharArray(),
        salt,
        iterations
    );
    Assertions.assertArrayEquals(recalculatedHash, hash);

    response = resource.getCachedSerializedUserMap(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof byte[]);
    Map<String, BasicAuthenticatorUser> cachedUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(objectMapper, (byte[]) response.getEntity());
    Assertions.assertNotNull(cachedUserMap);
    Assertions.assertNotNull(cachedUserMap.get("druid"));
    Assertions.assertEquals("druid", cachedUserMap.get("druid").getName());
    BasicAuthenticatorCredentials cachedUserCredentials = cachedUserMap.get("druid").getCredentials();

    salt = cachedUserCredentials.getSalt();
    hash = cachedUserCredentials.getHash();
    iterations = cachedUserCredentials.getIterations();
    Assertions.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assertions.assertEquals(PasswordHashGenerator.KEY_LENGTH / 8, hash.length);
    Assertions.assertEquals(BasicAuthUtils.DEFAULT_KEY_ITERATIONS, iterations);

    recalculatedHash = PasswordHashGenerator.computePasswordHash(
        "helloworld".toCharArray(),
        salt,
        iterations
    );
    Assertions.assertArrayEquals(recalculatedHash, hash);

    response = resource.deleteUser(mockHttpRequest(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(200, response.getStatus());

    response = resource.getUser(mockHttpRequestNoAudit(), AUTHENTICATOR_NAME, "druid");
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.updateUserCredentials(
        mockHttpRequestNoAudit(),
        AUTHENTICATOR_NAME,
        "druid",
        new BasicAuthenticatorCredentialUpdate("helloworld", null)
    );
    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  private HttpServletRequest mockHttpRequestNoAudit()
  {
    if (req != null) {
      EasyMock.verify(req);
    }
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.replay(req);
    return req;
  }

  private HttpServletRequest mockHttpRequest()
  {
    if (req != null) {
      EasyMock.verify(req);
    }
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").once();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("id", "authorizer", "authBy", Collections.emptyMap())
    ).once();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").once();
    EasyMock.expect(req.getMethod()).andReturn("GET").once();
    EasyMock.expect(req.getRequestURI()).andReturn("uri").once();
    EasyMock.expect(req.getQueryString()).andReturn("a=b").once();
    EasyMock.replay(req);

    return req;
  }

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }
}
