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

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.security.basic.authentication.BasicHTTPEscalator;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.storage.local.LocalFileExportStorageProvider;
import org.apache.druid.storage.s3.output.S3ExportStorageProvider;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedServiceClient;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.msq.MSQExportDirectory;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class BasicAuthMSQTest extends EmbeddedClusterTestBase
{
  public static final String USER_1 = "user1";
  public static final String ROLE_1 = "role1";
  public static final String USER_1_PASSWORD = "password1";

  /**
   * Attempts allowed for a request while basic-auth changes reach the Broker, which the
   * Coordinator propagates asynchronously (a push plus a poll every
   * {@code druid.auth.basic.common.pollingPeriod}).
   */
  private static final int AUTH_PROPAGATION_ATTEMPTS = 5;

  private SecurityClient securityClient;
  private EmbeddedServiceClient userClient;

  // Indexer with 2 slots, each with 150MB memory since minimum required memory
  // computed for the required tests is 133MB
  private final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000)
      .addProperty("druid.worker.capacity", "2");
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final MSQExportDirectory exportDirectory = new MSQExportDirectory();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(exportDirectory)
        .addResource(new EmbeddedBasicAuthResource())
        .addServer(new EmbeddedCoordinator())
        .addServer(overlord)
        .addServer(indexer)
        .addServer(new EmbeddedBroker())
        .addCommonProperty("druid.auth.basic.common.pollingPeriod", "100");
  }

  @BeforeAll
  public void setupClient()
  {
    // Make a custom client for testing out auth for the test user
    userClient = EmbeddedServiceClient.create(
        cluster,
        new BasicHTTPEscalator("basic", USER_1, new DefaultPasswordProvider(USER_1_PASSWORD))
    );

    // Use the default set of clients for calling security APIs
    securityClient = new SecurityClient(cluster.callApi().serviceClient());
  }

  @BeforeEach
  public void setupRoles()
  {
    // Authentication setup
    securityClient.createAuthenticationUser(USER_1);
    securityClient.setUserPassword(USER_1, USER_1_PASSWORD);

    // Authorizer setup
    securityClient.createAuthorizerUser(USER_1);
    securityClient.createAuthorizerRole(ROLE_1);
    securityClient.assignUserToRole(USER_1, ROLE_1);
  }

  @AfterEach
  public void tearDownRoles()
  {
    securityClient.deleteAuthenticationUser(USER_1);
    securityClient.deleteAuthorizerUser(USER_1);
    securityClient.deleteAuthorizerRole(ROLE_1);
  }

  @Test
  public void testIngestionWithoutPermissions() throws Exception
  {
    List<ResourceAction> permissions = ImmutableList.of();
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json().getAbsolutePath()
    );

    verifySqlSubmitFailsWith403Forbidden(queryLocal);
  }

  @Test
  public void testIngestionWithPermissions() throws Exception
  {
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String queryLocal = StringUtils.format(
        MoreResources.MSQ.INSERT_TINY_WIKI_JSON,
        dataSource,
        Resources.DataFile.tinyWiki1Json()
    );

    final SqlTaskStatus taskStatus = submitSqlTaskWhenAuthorized(queryLocal);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
  }

  @Test
  public void testExportWithoutPermissions() throws Exception
  {
    // No external write permissions for s3
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource(S3ExportStorageProvider.TYPE_NAME, "EXTERNAL"), Action.WRITE),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String exportQuery =
        StringUtils.format(
            """
                INSERT INTO extern(%s(exportPath => '%s'))
                AS CSV
                SELECT page, added, delta
                FROM TABLE(
                  EXTERN(
                    '{"type":"local","files":["%s"]}',
                    '{"type":"json"}',
                    '[{"type":"string","name":"timestamp"},{"type":"string","name":"isRobot"},{"type":"string","name":"diffUrl"},{"type":"long","name":"added"},{"type":"string","name":"countryIsoCode"},{"type":"string","name":"regionName"},{"type":"string","name":"channel"},{"type":"string","name":"flags"},{"type":"long","name":"delta"},{"type":"string","name":"isUnpatrolled"},{"type":"string","name":"isNew"},{"type":"double","name":"deltaBucket"},{"type":"string","name":"isMinor"},{"type":"string","name":"isAnonymous"},{"type":"long","name":"deleted"},{"type":"string","name":"cityName"},{"type":"long","name":"metroCode"},{"type":"string","name":"namespace"},{"type":"string","name":"comment"},{"type":"string","name":"page"},{"type":"long","name":"commentLength"},{"type":"string","name":"countryName"},{"type":"string","name":"user"},{"type":"string","name":"regionIsoCode"}]'
                  )
                )
                """,
            LocalFileExportStorageProvider.TYPE_NAME,
            cluster.getTestFolder().getOrCreateFolder("msq-export").getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    verifySqlSubmitFailsWith403Forbidden(exportQuery);
  }

  @Test
  public void testExportWithPermissions() throws Exception
  {
    // No external write permissions for s3
    List<ResourceAction> permissions = ImmutableList.of(
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.READ),
        new ResourceAction(new Resource("EXTERNAL", "EXTERNAL"), Action.READ),
        new ResourceAction(new Resource(LocalFileExportStorageProvider.TYPE_NAME, "EXTERNAL"), Action.WRITE),
        new ResourceAction(new Resource("STATE", "STATE"), Action.READ),
        new ResourceAction(new Resource(".*", "DATASOURCE"), Action.WRITE)
    );
    securityClient.setPermissionsToRole(ROLE_1, permissions);

    String exportQuery =
        StringUtils.format(
            """
                INSERT INTO extern(%s(exportPath => '%s'))
                AS CSV
                SELECT page, added, delta
                FROM TABLE(
                  EXTERN(
                    '{"type":"local","files":["%s"]}',
                    '{"type":"json"}',
                    '[{"type":"string","name":"timestamp"},{"type":"string","name":"isRobot"},{"type":"string","name":"diffUrl"},{"type":"long","name":"added"},{"type":"string","name":"countryIsoCode"},{"type":"string","name":"regionName"},{"type":"string","name":"channel"},{"type":"string","name":"flags"},{"type":"long","name":"delta"},{"type":"string","name":"isUnpatrolled"},{"type":"string","name":"isNew"},{"type":"double","name":"deltaBucket"},{"type":"string","name":"isMinor"},{"type":"string","name":"isAnonymous"},{"type":"long","name":"deleted"},{"type":"string","name":"cityName"},{"type":"long","name":"metroCode"},{"type":"string","name":"namespace"},{"type":"string","name":"comment"},{"type":"string","name":"page"},{"type":"long","name":"commentLength"},{"type":"string","name":"countryName"},{"type":"string","name":"user"},{"type":"string","name":"regionIsoCode"}]'
                  )
                )
                """,
            LocalFileExportStorageProvider.TYPE_NAME,
            new File(exportDirectory.get(), dataSource).getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json()
        );

    final SqlTaskStatus taskStatus = submitSqlTaskWhenAuthorized(exportQuery);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
  }

  /**
   * Submits an MSQ task to an arbitrary Broker as {@link #USER_1}.
   */
  private SqlTaskStatus submitSqlTaskAsUser(String sql)
  {
    return userClient.onAnyBroker(
        b -> b.submitSqlTask(
            new ClientSqlQuery(sql, null, false, false, false, Map.of(), List.of())
        )
    );
  }

  /**
   * Submits an MSQ task as {@link #USER_1}, retrying on a transient 401/403 while the Broker's
   * auth cache catches up with the test setup.
   */
  private SqlTaskStatus submitSqlTaskWhenAuthorized(String sql) throws Exception
  {
    return RetryUtils.retry(
        () -> submitSqlTaskAsUser(sql),
        BasicAuthMSQTest::isTransientAuthFailure,
        AUTH_PROPAGATION_ATTEMPTS
    );
  }

  /**
   * Asserts that submitting the SQL as an unauthorized user fails with 403 Forbidden. Retries on a
   * transient 401 (authentication not yet propagated); 403 is the expected result, so it is not.
   */
  private void verifySqlSubmitFailsWith403Forbidden(String sql)
  {
    try {
      RetryUtils.retry(
          () -> submitSqlTaskAsUser(sql),
          e -> failedWithHttpError(e, "401 Unauthorized"),
          AUTH_PROPAGATION_ATTEMPTS
      );
      Assertions.fail("Expected submit to fail with 403 Forbidden");
    }
    catch (Exception e) {
      MatcherAssert.assertThat(e, ExceptionMatcher.of(Exception.class).expectMessageContains("403 Forbidden"));
    }
  }

  /**
   * Whether the error is a transient 401 (new user) or 403 (new permission) from the Broker's
   * auth cache lagging behind the test setup.
   */
  private static boolean isTransientAuthFailure(Throwable t)
  {
    return failedWithHttpError(t, "401 Unauthorized") || failedWithHttpError(t, "403 Forbidden");
  }

  private static boolean failedWithHttpError(Throwable t, String httpError)
  {
    return ExceptionMatcher.of(Exception.class).expectMessageContains(httpError).matches(t);
  }
}
