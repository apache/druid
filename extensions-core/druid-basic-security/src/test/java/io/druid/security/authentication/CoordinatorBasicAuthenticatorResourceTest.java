/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.metadata.DefaultPasswordProvider;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.BasicAuthCommonCacheConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.BasicHTTPEscalator;
import io.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.authentication.endpoint.BasicAuthenticatorResource;
import io.druid.security.basic.authentication.endpoint.CoordinatorBasicAuthenticatorResourceHandler;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import io.druid.server.DruidNode;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.server.security.Escalator;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;

public class CoordinatorBasicAuthenticatorResourceTest
{
  private final static String AUTHENTICATOR_NAME = "test";
  private final static String AUTHENTICATOR_NAME2 = "test2";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private BasicAuthenticatorResource resource;
  private CoordinatorBasicAuthenticatorMetadataStorageUpdater storageUpdater;
  private HttpServletRequest req;

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);

    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    ObjectMapper objectMapper = new ObjectMapper(new SmileFactory());

    AuthenticatorMapper authenticatorMapper = new AuthenticatorMapper(
        ImmutableMap.of(
            AUTHENTICATOR_NAME,
            new BasicHTTPAuthenticator(
                null,
                AUTHENTICATOR_NAME,
                "test",
                new DefaultPasswordProvider("druid"),
                new DefaultPasswordProvider("druid"),
                null,
                null,
                null
            ),
            AUTHENTICATOR_NAME2,
            new BasicHTTPAuthenticator(
                null,
                AUTHENTICATOR_NAME2,
                "test",
                new DefaultPasswordProvider("druid"),
                new DefaultPasswordProvider("druid"),
                null,
                null,
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
        )
    );

    storageUpdater.start();
  }

  @After
  public void tearDown() throws Exception
  {
    storageUpdater.stop();
  }

  @Test
  public void testInvalidAuthenticator()
  {
    Response response = resource.getAllUsers(req, "invalidName");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        errorMapWithMsg("Basic authenticator with name [invalidName] does not exist."),
        response.getEntity()
    );
  }

  @Test
  public void testGetAllUsers()
  {
    Response response = resource.getAllUsers(req, AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME), response.getEntity());

    resource.createUser(req, AUTHENTICATOR_NAME, "druid");
    resource.createUser(req, AUTHENTICATOR_NAME, "druid2");
    resource.createUser(req, AUTHENTICATOR_NAME, "druid3");

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "druid",
        "druid2",
        "druid3"
    );

    response = resource.getAllUsers(req, AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME), response.getEntity());

    resource.createUser(req, AUTHENTICATOR_NAME, "druid");
    resource.createUser(req, AUTHENTICATOR_NAME, "druid2");
    resource.createUser(req, AUTHENTICATOR_NAME, "druid3");

    resource.createUser(req, AUTHENTICATOR_NAME2, "druid4");
    resource.createUser(req, AUTHENTICATOR_NAME2, "druid5");
    resource.createUser(req, AUTHENTICATOR_NAME2, "druid6");

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

    response = resource.getAllUsers(req, AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, AUTHENTICATOR_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    BasicAuthenticatorUser expectedUser = new BasicAuthenticatorUser("druid", null);
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.getUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testUserCredentials()
  {
    Response response = resource.createUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.updateUserCredentials(
        req,
        AUTHENTICATOR_NAME,
        "druid",
        new BasicAuthenticatorCredentialUpdate("helloworld", null)
    );
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    BasicAuthenticatorUser actualUser = (BasicAuthenticatorUser) response.getEntity();
    Assert.assertEquals("druid", actualUser.getName());
    BasicAuthenticatorCredentials credentials = actualUser.getCredentials();

    byte[] salt = credentials.getSalt();
    byte[] hash = credentials.getHash();
    int iterations = credentials.getIterations();
    Assert.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assert.assertEquals(BasicAuthUtils.KEY_LENGTH / 8, hash.length);
    Assert.assertEquals(BasicAuthUtils.DEFAULT_KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        salt,
        iterations
    );
    Assert.assertArrayEquals(recalculatedHash, hash);

    response = resource.deleteUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.updateUserCredentials(
        req,
        AUTHENTICATOR_NAME,
        "druid",
        new BasicAuthenticatorCredentialUpdate("helloworld", null)
    );
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }

  private Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", null, null, true, false)
                );

                binder.bind(Escalator.class).toInstance(
                    new BasicHTTPEscalator(null, null, null)
                );
              }
            }
        )
    );
  }
}
