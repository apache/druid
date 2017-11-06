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

package io.druid.security.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicAuthenticatorResource;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.server.security.AllowAllAuthenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class BasicAuthenticatorResourceTest
{
  private static final String BASIC_AUTHENTICATOR_NAME = "basic";
  private static final String BASIC_AUTHENTICATOR_NAME2 = "basic2";

  private BasicAuthenticatorResource resource;
  private HttpServletRequest req;
  private TestDerbyAuthenticatorStorageConnector connector;

  @Rule
  public final TestDerbyAuthenticatorStorageConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyAuthenticatorStorageConnector.DerbyConnectorRule("test");


  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    connector = derbyConnectorRule.getConnector();

    AuthenticatorMapper mapper = new AuthenticatorMapper(
        ImmutableMap.of(
            BASIC_AUTHENTICATOR_NAME, new BasicHTTPAuthenticator(connector, BASIC_AUTHENTICATOR_NAME, "druid", "druid", "druid"),
            BASIC_AUTHENTICATOR_NAME2, new BasicHTTPAuthenticator(connector, BASIC_AUTHENTICATOR_NAME2, "druid", "druid", "druid"),
            "allowAll", new AllowAllAuthenticator()
        ),
        "basic"
    );

    createAllTables();
    resource = new BasicAuthenticatorResource(connector, mapper);
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, BASIC_AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid2");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid3");

    resource.createUser(req, BASIC_AUTHENTICATOR_NAME2, "druid4");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME2, "druid5");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME2, "druid6");

    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );
    List<Map<String, Object>> expectedUsers2 = ImmutableList.of(
        ImmutableMap.of("name", "druid4"),
        ImmutableMap.of("name", "druid5"),
        ImmutableMap.of("name", "druid6")
    );

    response = resource.getAllUsers(req, BASIC_AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, BASIC_AUTHENTICATOR_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());
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
    Response response = resource.getAllUsers(req, BASIC_AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid2");
    resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid3");

    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );

    response = resource.getAllUsers(req, BASIC_AUTHENTICATOR_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> expectedUser = ImmutableMap.of(
        "user", ImmutableMap.of("name", "druid")
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.getUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  @Test
  public void testUserCredentials()
  {
    Response response = resource.createUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.updateUserCredentials(req, BASIC_AUTHENTICATOR_NAME, "druid", "helloworld");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());
    Map<String, Object> ent = (Map<String, Object>) response.getEntity();
    Map<String, Object> credentials = (Map<String, Object>) ent.get("credentials");
    Assert.assertEquals(
        ImmutableMap.of("name", "druid"),
        ent.get("user")
    );

    byte[] salt = (byte[]) credentials.get("salt");
    byte[] hash = (byte[]) credentials.get("hash");
    int iterations = (Integer) credentials.get("iterations");
    Assert.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assert.assertEquals(BasicAuthUtils.KEY_LENGTH / 8, hash.length);
    Assert.assertEquals(BasicAuthUtils.KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        salt,
        iterations
    );
    Assert.assertArrayEquals(recalculatedHash, hash);

    response = resource.deleteUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, BASIC_AUTHENTICATOR_NAME, "druid");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());

    response = resource.updateUserCredentials(req, BASIC_AUTHENTICATOR_NAME, "druid", "helloworld");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [druid] does not exist."), response.getEntity());
  }

  private void createAllTables()
  {
    connector.createUserTable(BASIC_AUTHENTICATOR_NAME);
    connector.createUserCredentialsTable(BASIC_AUTHENTICATOR_NAME);

    connector.createUserTable(BASIC_AUTHENTICATOR_NAME2);
    connector.createUserCredentialsTable(BASIC_AUTHENTICATOR_NAME2);
  }

  private void dropAllTables()
  {
    for (String table : connector.getTableNamesForPrefix(BASIC_AUTHENTICATOR_NAME)) {
      dropTable(table);
    }

    for (String table : connector.getTableNamesForPrefix(BASIC_AUTHENTICATOR_NAME2)) {
      dropTable(table);
    }
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }
}
