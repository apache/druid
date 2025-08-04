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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.msq.guice.IndexerMemoryManagementModule;
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.guice.MSQSqlModule;
import org.apache.druid.msq.guice.SqlTaskModule;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.security.basic.BasicSecurityDruidModule;
import org.apache.druid.security.basic.authentication.BasicHTTPEscalator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.local.LocalFileExportStorageProvider;
import org.apache.druid.storage.s3.output.S3ExportStorageProvider;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.EmbeddedServiceClient;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.msq.MsqExportDirectory;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class BasicAuthMsqTest extends EmbeddedClusterTestBase
{
  private SecurityClient securityClient;

  public static final String USER_1 = "user1";
  public static final String ROLE_1 = "role1";
  public static final String USER_1_PASSWORD = "password1";

  // Time in ms to sleep after updating role permissions in each test. This intends to give the
  // underlying test cluster enough time to sync permissions and be ready when test execution starts.
  private static final int SYNC_SLEEP = 500;

  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(400_000_000)
      .addProperty("druid.worker.capacity", "2");
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private EmbeddedServiceClient userClient;
  private final MsqExportDirectory exportDirectory = new MsqExportDirectory();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addResource(exportDirectory)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(historical)
        .addServer(broker)
        .addServer(new EmbeddedRouter())
        .addExtensions(
            BasicSecurityDruidModule.class,
            MSQSqlModule.class,
            MSQIndexingModule.class,
            SqlTaskModule.class,
            MSQDurableStorageModule.class,
            MSQExternalDataSourceModule.class,
            IndexerMemoryManagementModule.class
        )
        .addCommonProperty("druid.auth.basic.common.pollingPeriod", "100")
        .addCommonProperty("druid.auth.authenticatorChain", "[\"basic\"]")
        .addCommonProperty("druid.auth.authenticator.basic.type", "basic")
        .addCommonProperty("druid.auth.authenticator.basic.initialAdminPassword", "priest")
        .addCommonProperty("druid.auth.authenticator.basic.initialInternalClientPassword", "warlock")
        .addCommonProperty("druid.auth.authenticator.basic.authorizerName", "basic")
        .addCommonProperty("druid.auth.authorizers", "[\"basic\"]")
        .addCommonProperty("druid.auth.authorizer.basic.type", "basic")
        .addCommonProperty("druid.escalator.type", "basic")
        .addCommonProperty("druid.escalator.internalClientPassword", "warlock")
        .addCommonProperty("druid.escalator.internalClientUsername", "druid_system")
        .addCommonProperty("druid.escalator.authorizerName", "basic");
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

    waitForPermissionsToSync();

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n",
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

    waitForPermissionsToSync();

    String queryLocal =
        StringUtils.format(
            "INSERT INTO %s\n"
            + "SELECT\n"
            + "  TIME_PARSE(\"timestamp\") AS __time,\n"
            + "  isRobot,\n"
            + "  diffUrl,\n"
            + "  added,\n"
            + "  countryIsoCode,\n"
            + "  regionName,\n"
            + "  channel,\n"
            + "  flags,\n"
            + "  delta,\n"
            + "  isUnpatrolled,\n"
            + "  isNew,\n"
            + "  deltaBucket,\n"
            + "  isMinor,\n"
            + "  isAnonymous,\n"
            + "  deleted,\n"
            + "  cityName,\n"
            + "  metroCode,\n"
            + "  namespace,\n"
            + "  comment,\n"
            + "  page,\n"
            + "  commentLength,\n"
            + "  countryName,\n"
            + "  user,\n"
            + "  regionIsoCode\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n"
            + "PARTITIONED BY DAY\n",
            dataSource,
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    final SqlTaskStatus taskStatus = userClient.onAnyBroker(
        b -> b.submitSqlTask(
            new ClientSqlQuery(queryLocal, null, false, false, false, Map.of(), List.of())
        )
    );
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
  }

  @Test
  public void testExportWithoutPermissions() throws InterruptedException
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

    waitForPermissionsToSync();

    String exportQuery =
        StringUtils.format(
            "INSERT INTO extern(%s(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            LocalFileExportStorageProvider.TYPE_NAME,
            cluster.getTestFolder().getOrCreateFolder("msq-export").getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json().getAbsolutePath()
        );

    verifySqlSubmitFailsWith403Forbidden(exportQuery);
  }

  @Test
  public void testExportWithPermissions() throws InterruptedException
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

    waitForPermissionsToSync();

    String exportQuery =
        StringUtils.format(
            "INSERT INTO extern(%s(exportPath => '%s'))\n"
            + "AS CSV\n"
            + "SELECT page, added, delta\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{\"type\":\"local\",\"files\":[\"%s\"]}',\n"
            + "    '{\"type\":\"json\"}',\n"
            + "    '[{\"type\":\"string\",\"name\":\"timestamp\"},{\"type\":\"string\",\"name\":\"isRobot\"},{\"type\":\"string\",\"name\":\"diffUrl\"},{\"type\":\"long\",\"name\":\"added\"},{\"type\":\"string\",\"name\":\"countryIsoCode\"},{\"type\":\"string\",\"name\":\"regionName\"},{\"type\":\"string\",\"name\":\"channel\"},{\"type\":\"string\",\"name\":\"flags\"},{\"type\":\"long\",\"name\":\"delta\"},{\"type\":\"string\",\"name\":\"isUnpatrolled\"},{\"type\":\"string\",\"name\":\"isNew\"},{\"type\":\"double\",\"name\":\"deltaBucket\"},{\"type\":\"string\",\"name\":\"isMinor\"},{\"type\":\"string\",\"name\":\"isAnonymous\"},{\"type\":\"long\",\"name\":\"deleted\"},{\"type\":\"string\",\"name\":\"cityName\"},{\"type\":\"long\",\"name\":\"metroCode\"},{\"type\":\"string\",\"name\":\"namespace\"},{\"type\":\"string\",\"name\":\"comment\"},{\"type\":\"string\",\"name\":\"page\"},{\"type\":\"long\",\"name\":\"commentLength\"},{\"type\":\"string\",\"name\":\"countryName\"},{\"type\":\"string\",\"name\":\"user\"},{\"type\":\"string\",\"name\":\"regionIsoCode\"}]'\n"
            + "  )\n"
            + ")\n",
            LocalFileExportStorageProvider.TYPE_NAME,
            new File(exportDirectory.get(), dataSource).getAbsolutePath(),
            Resources.DataFile.tinyWiki1Json()
        );

    final SqlTaskStatus taskStatus = userClient.onAnyBroker(
        b -> b.submitSqlTask(
            new ClientSqlQuery(exportQuery, null, false, false, false, Map.of(), List.of())
        )
    );
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), overlord);
  }

  @Test
  public void testExportTaskSubmitOverlordWithPermission() throws Exception
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

    waitForPermissionsToSync();

    final String taskId = IdUtils.getRandomId();
    userClient.onLeaderOverlord(o -> o.runTask(taskId, createExportTask(taskId)));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
  }

  @Test
  public void testExportTaskSubmitOverlordWithoutPermission() throws Exception
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

    waitForPermissionsToSync();

    final String taskId = IdUtils.getRandomId();
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> userClient.onLeaderOverlord(o -> o.runTask(taskId, createExportTask(taskId)))
        ),
        ExceptionMatcher.of(Exception.class).expectMessageContains("403 Forbidden")
    );
  }

  private Task createExportTask(String taskId)
  {
    final RowSignature rowSignature = RowSignature
        .builder()
        .add("timestamp", ColumnType.STRING)
        .add("isRobot", ColumnType.STRING)
        .add("diffUrl", ColumnType.STRING)
        .add("added", ColumnType.LONG)
        .add("countryIsoCode", ColumnType.STRING)
        .add("regionName", ColumnType.STRING)
        .add("channel", ColumnType.STRING)
        .add("flags", ColumnType.STRING)
        .add("delta", ColumnType.LONG)
        .add("isUnpatrolled", ColumnType.STRING)
        .add("isNew", ColumnType.STRING)
        .add("deltaBucket", ColumnType.DOUBLE)
        .add("isMinor", ColumnType.STRING)
        .add("isAnonymous", ColumnType.STRING)
        .add("deleted", ColumnType.LONG)
        .add("cityName", ColumnType.STRING)
        .add("metroCode", ColumnType.LONG)
        .add("namespace", ColumnType.STRING)
        .add("comment", ColumnType.STRING)
        .add("page", ColumnType.STRING)
        .add("commentLength", ColumnType.LONG)
        .add("countryName", ColumnType.STRING)
        .add("user", ColumnType.STRING)
        .add("regionIsoCode", ColumnType.STRING)
        .build();

    return new MSQControllerTask(
        taskId,
        new LegacyMSQSpec(
            new Druids.ScanQueryBuilder()
                .columns("added", "delta", "page")
                .dataSource(
                    new ExternalDataSource(
                        new LocalInputSource(null, null, List.of(Resources.DataFile.tinyWiki1Json()), null),
                        new JsonInputFormat(null, null, null, null, null),
                        rowSignature
                    )
                )
                .eternityInterval()
                .context(
                    Map.of(
                        "scanSignature",
                        "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"page\",\"type\":\"STRING\"}]"
                    )
                )
                .build(),
            new ColumnMappings(
                List.of(
                    new ColumnMapping("page", "page"),
                    new ColumnMapping("added", "added"),
                    new ColumnMapping("delta", "delta")
                )
            ),
            new ExportMSQDestination(
                new LocalFileExportStorageProvider(new File(exportDirectory.get(), dataSource).getAbsolutePath()),
                ResultFormat.CSV
            ),
            WorkerAssignmentStrategy.MAX,
            MSQTuningConfig.defaultConfig()
        ),
        null,
        null,
        null,
        List.of(SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.BIGINT),
        List.of(ColumnType.STRING, ColumnType.LONG, ColumnType.LONG),
        null,
        null
    );
  }

  private void waitForPermissionsToSync() throws InterruptedException
  {
    Thread.sleep(SYNC_SLEEP);
  }

  private void verifySqlSubmitFailsWith403Forbidden(String sql)
  {
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            Exception.class,
            () -> userClient.onAnyBroker(
                b -> b.submitSqlTask(
                    new ClientSqlQuery(sql, null, false, false, false, Map.of(), List.of())
                )
            )
        ),
        ExceptionMatcher.of(Exception.class).expectMessageContains("403 Forbidden")
    );
  }
}
