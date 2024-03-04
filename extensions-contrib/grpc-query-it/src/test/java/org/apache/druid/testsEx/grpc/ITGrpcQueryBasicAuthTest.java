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

package org.apache.druid.testsEx.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.druid.grpc.TestClient;
import org.apache.druid.grpc.proto.QueryOuterClass;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testsEx.categories.GrpcQueryBasicAuth;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.security.BasicAuthHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(DruidTestRunner.class)
@Category(GrpcQueryBasicAuth.class)
public class ITGrpcQueryBasicAuthTest extends GrpcQueryTestBase
{
  public static final String ADMIN_USER = "admin";
  // This password must match that set up in the basic-auth.env config.
  public static final String ADMIN_PASSWORD = "priest";

  public static final String USER_ALICE = "alice";
  public static final String ALICE_PASSWORD = "alicePwd";

  public static final String USER_BOB = "bob";
  public static final String BOB_PASSWORD = "bobPwd";

  protected static final List<ResourceAction> ALICE_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource("servers", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),

      // Required to allow access to system tables.
      // https://druid.apache.org/docs/latest/operations/security-user-auth.html#system_table
      new ResourceAction(
          new Resource(".*", ResourceType.STATE),
          Action.READ
      )
  );

  protected static final List<ResourceAction> BOB_PERMISSIONS = ImmutableList.of(
      new ResourceAction(
          new Resource("segments", ResourceType.SYSTEM_TABLE),
          Action.READ
      ),
      new ResourceAction(
          new Resource(".*", ResourceType.STATE),
          Action.READ
      )
  );

  private static boolean initialized;
  private static HttpClient adminClient;
  private static BasicAuthHelper authHelper;

  @Inject
  protected IntegrationTestingConfig config;

  @Inject
  protected ObjectMapper jsonMapper;

  @Inject
  @Client
  protected HttpClient httpClient;

  @Inject
  protected CoordinatorResourceTestClient coordinatorClient;

  @BeforeClass
  @AfterClass
  public static void reset()
  {
    initialized = false;
    adminClient = null;
    authHelper = null;
  }

  @Before
  public void setup() throws Exception
  {
    if (!initialized) {
      adminClient = new CredentialedHttpClient(
          new BasicCredentials(ADMIN_USER, ADMIN_PASSWORD),
          httpClient
      );
      authHelper = new BasicAuthHelper(
          adminClient,
          config.getCoordinatorUrl(),
          jsonMapper
      );
      authHelper.cleanupUsersAndRoles();
      createUsers();
      initialized = true;
    }
  }

  private void createUsers()
  {
    authHelper.createUserAndRoleWithPermissions(USER_ALICE, ALICE_PASSWORD, "aliceRole", ALICE_PERMISSIONS);
    authHelper.createUserAndRoleWithPermissions(USER_BOB, BOB_PASSWORD, "bobRole", BOB_PERMISSIONS);
  }

  @Test
  public void testAdminUser()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, ADMIN_USER, ADMIN_PASSWORD)) {
      testCsv(client);
    }
  }

  @Test
  public void testInvalidUser()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, "bogus", "invalid")) {
      QueryRequest request = QueryRequest.newBuilder()
          .setQuery(SQL)
          .setQueryType(QueryOuterClass.QueryType.SQL)
          .setResultFormat(QueryResultFormat.CSV)
          .build();
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.client().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  @Test
  public void testInvalidPassword()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, ADMIN_USER, "invalid")) {
      QueryRequest request = QueryRequest.newBuilder()
          .setQuery(SQL)
          .setQueryType(QueryOuterClass.QueryType.SQL)
          .setResultFormat(QueryResultFormat.CSV)
          .build();
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.client().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  private static final String SERVERS_SQL = "SELECT server, host FROM sys.servers";

  @Test
  public void testUnauthUser()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, USER_BOB, BOB_PASSWORD)) {
      QueryRequest request = QueryRequest.newBuilder()
          .setQuery(SERVERS_SQL)
          .setQueryType(QueryOuterClass.QueryType.SQL)
          .setResultFormat(QueryResultFormat.CSV)
          .build();
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> client.client().submitQuery(request));
      assertEquals(Status.PERMISSION_DENIED, e.getStatus());
    }
  }

  @Test
  public void testNormalUser()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, USER_ALICE, ALICE_PASSWORD)) {
      QueryRequest request = QueryRequest.newBuilder()
          .setQuery(SERVERS_SQL)
          .setQueryType(QueryOuterClass.QueryType.SQL)
          .setResultFormat(QueryResultFormat.CSV)
          .build();
      QueryResponse response = client.client().submitQuery(request);
      assertEquals(QueryStatus.OK, response.getStatus());
      String data = response.getData().toString(StandardCharsets.UTF_8);
      String[] lines = data.split("\n");
      assertEquals(6, lines.length);
    }
  }

  @Test
  public void testProtobuf()
  {
    try (TestClient client = new TestClient(GRPC_ENDPOINT, USER_ALICE, ALICE_PASSWORD)) {
      testProtobuf(client);
    }
  }
}
